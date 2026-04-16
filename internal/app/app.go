package app

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/dnstapir/tapir-analyse-lib/common"
	"github.com/dnstapir/tapir-analyse-lib/libtapir"

	"github.com/dnstapir/tapir-analyse-listchecker/internal/checker"
)

const c_N_HANDLERS = 3
const c_NATS_DELIM = common.NATS_DELIM

type Conf struct {
	Debug       bool   `toml:"debug"`
	Interval    int    `toml:"interval"`
	Observation string `toml:"observation"`
	ListUrl     string `toml:"list_url"`
	FullMatch   bool   `toml:"full_match"`
	checker.Conf
	AnalystID     string
	Log           common.Logger
	NatsHandle    nats
	FetcherHandle fetcher
}

type appHandle struct {
	id            string
	observation   string
	log           common.Logger
	fullMatch     bool
	checkerHandle *checker.Checker
	natsHandle    nats
	fetcherHandle fetcher
	listUrl       string
	ticker        *time.Ticker
	exitCh        chan<- common.Exit
	pm
}

type pm struct {
}

type job struct {
	isTick   bool
	tickData int64
	msg      common.NatsMsg
}

type nats interface {
	ActivateSubscription(context.Context) (<-chan common.NatsMsg, error)
	SetObservation(context.Context, string, string) error
	Shutdown() error
}

type fetcher interface {
	Fetch(string) (<-chan string, error)
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}
	a.log = conf.Log

	if conf.NatsHandle == nil {
		return nil, common.ErrBadHandle
	}
	a.natsHandle = conf.NatsHandle

	if conf.FetcherHandle == nil {
		return nil, common.ErrBadHandle
	}
	a.fetcherHandle = conf.FetcherHandle

	if conf.AnalystID == "" {
		a.log.Error("Bad analyst ID when creating listchecker")
		return nil, common.ErrBadParam
	}
	a.id = conf.AnalystID

	if conf.ListUrl == "" {
		a.log.Error("Bad list URL when creating listchecker")
		return nil, common.ErrBadParam
	}
	a.listUrl = conf.ListUrl

	if conf.Observation == "" {
		a.log.Error("Bad observation when creating listchecker")
		return nil, common.ErrBadParam
	}
	a.observation = conf.Observation

	if conf.Interval > 0 {
		a.ticker = time.NewTicker(time.Duration(conf.Interval) * time.Second)
	} else {
		a.ticker = time.NewTicker(time.Duration(math.MaxInt32) * time.Second)
		a.ticker.Stop()
		a.log.Warning("No interval set. Won't refresh list.")
	}

	c, err := checker.Create(conf.Conf)
	if err != nil {
		a.log.Error("Could not create checker handle: %s", err)
		return nil, err
	}
	a.checkerHandle = c

	a.fullMatch = conf.FullMatch
	if a.fullMatch {
		a.log.Info("Will match full domain names against list")
	} else {
		a.log.Info("Will only match effective TLD plus one label against list")
	}

	a.log.Debug("Main app debug logging enabled")
	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	defer a.ticker.Stop()

	var natsChan <-chan common.NatsMsg
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

	fetchCh, err := a.fetcherHandle.Fetch(a.listUrl)
	if err != nil {
		a.log.Error("Couldn't run initial fetch of list data: %s", err)
	}

	for d := range fetchCh {
		a.log.Debug("Got domain %q from list", d) // TODO remove
		a.checkerHandle.Add(libtapir.NormalizeDomainName(d))
	}

	natsChan, err = a.natsHandle.ActivateSubscription(ctx)
	if err != nil {
		a.log.Error("Couldn't activate NATS subscription: '%s'", err)
		a.exitCh <- common.Exit{ID: a.id, Err: err}
		return
	}

	var wg sync.WaitGroup
	for range c_N_HANDLERS {
		wg.Go(func() {
			for j := range jobChan {
				a.handleJob(ctx, j)
			}
			a.log.Info("Worker done!")
		})
	}

	a.log.Info("Starting main app loop")
MAIN_APP_LOOP:
	for {
		select {
		case t := <-a.ticker.C:
			a.log.Debug("Tick")
			j := job{
				isTick:   true,
				tickData: t.Unix(),
			}
			jobChan <- j
		case natsMsg, ok := <-natsChan:
			if !ok {
				a.log.Warning("NATS channel closed")
				natsChan = nil
			} else {
				a.log.Debug("Incoming NATS message")
				j := job{
					msg: natsMsg,
				}
				jobChan <- j
			}
		case <-ctx.Done():
			a.log.Info("Stopping main worker thread")
			break MAIN_APP_LOOP
		}
	}

	close(jobChan)

	wg.Wait()

	err = a.natsHandle.Shutdown()
	if err != nil {
		a.log.Error("Encountered '%s' during NATS shutdown", err)
	}

	a.exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("Main app shutdown done")
	return
}

func (a *appHandle) handleJob(ctx context.Context, j job) {
	if j.isTick {
		a.handleTick(ctx, j.tickData)
	} else {
		a.handleMsg(ctx, j.msg)
	}
}

func (a *appHandle) handleTick(ctx context.Context, epoch int64) {
	a.log.Debug("Received tick '%d'", epoch)
	if epoch == 0 {
		a.log.Debug("Tick had zero value, probably garbage. Won't handle...")
		return
	}

	fetchCh, err := a.fetcherHandle.Fetch(a.listUrl)
	if err != nil {
		a.log.Error("Couldn't fetch new list data: %s", err)
		return
	}

	a.checkerHandle.Empty()

	a.log.Debug("Re-populating checker")
	for d := range fetchCh {
		a.log.Debug("Got domain %q from renewed list", d)
		a.checkerHandle.Add(libtapir.NormalizeDomainName(d))
	}
}

func (a *appHandle) handleMsg(ctx context.Context, msg common.NatsMsg) {
	a.log.Debug("Handling %d byte message on subject %s", len(msg.Data), msg.Subject)
	if len(msg.Data) <= 0 {
		a.log.Warning("Msg had no data, probably garbage. Won't handle...")
		return
	}

	// TODO schema validation

	msgDomain, err := libtapir.ExtractDomain(msg.Data)
	if err != nil {
		a.log.Error("Error reading domain from message: %s", err)
		return
	}

	if !libtapir.HasValidETLD(msgDomain) {
		a.log.Debug("Domain %q did not have valid eTLD, ignoring...", msgDomain)
		return
	}

	domainToCheck := msgDomain
	if !a.fullMatch {
		etldPlusOne, err := libtapir.GetETLDPlusOne(msgDomain)
		if err != nil {
			a.log.Warning("Could not get ETLD+1 from domain, using full name instead")
		} else {
			domainToCheck = etldPlusOne
			a.log.Debug("ETLD+1 for %q is %q", msgDomain, domainToCheck)
		}
	}

	if !a.checkerHandle.Check(domainToCheck) {
		a.log.Debug("Domain %q was not on list", domainToCheck)
		return
	}

	err = a.natsHandle.SetObservation(ctx, msgDomain, a.observation)
	if err != nil {
		a.log.Error("Error setting observation '%s' for '%s': %s", a.observation, msgDomain, err)
	} else {
		a.log.Debug("Observation '%s' set for '%s'", a.observation, msgDomain)
	}
}
