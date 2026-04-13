package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dnstapir/tapir-analyse-lib/common"
)

type Conf struct {
	Active  bool   `toml:"active"`
	Debug   bool   `toml:"debug"`
	Address string `toml:"address"`
	Port    string `toml:"port"`
	Log     common.Logger
	App     appHandle
}

type apiHandle struct {
	active          bool
	id              string
	log             common.Logger
	listenInterface string
	app             appHandle
	srv             http.Server
}

type appHandle interface {
}

func Create(conf Conf) (*apiHandle, error) {
	a := new(apiHandle)
	a.id = "api"

	if !conf.Active {
		a.active = conf.Active
		return a, nil
	}

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.App == nil {
		return nil, common.ErrBadHandle
	}

	if conf.Address == "" {
		return nil, common.ErrBadParam
	}

	if conf.Port == "" {
		return nil, common.ErrBadParam
	}

	a.log = conf.Log
	a.app = conf.App
	a.listenInterface = net.JoinHostPort(conf.Address, conf.Port)
	a.active = conf.Active

	a.log.Debug("API debug logging enabled")
	return a, nil
}

func (a *apiHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	if !a.active {
		exitCh <- common.Exit{ID: a.id, Err: nil}
		return
	}

	srv := &http.Server{
		Addr:         a.listenInterface,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}

	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			a.log.Info("API server closing")
			err = nil
		} else {
			a.log.Error("Unexpected API server shutdown: '%s'", err)
		}

	}()

	<-ctx.Done()
	a.log.Info("Shutting down API")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	if err != nil {
		a.log.Error("Bad API server shutdown: '%s'", err)
	}
	wg.Wait()

	exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("API server shutdown done")
	return
}
