package fetcher

import (
	"bufio"
	"net/http"
	"net/url"
	"strings"

	"github.com/dnstapir/tapir-analyse-lib/common"
	"github.com/dnstapir/tapir-analyse-lib/logger"
)

type fetcher struct {
	log common.Logger
}

type Conf struct {
	Debug bool `toml:"debug"`
	Log   common.Logger
}

func Create(conf Conf) (*fetcher, error) {
	f := new(fetcher)

	if conf.Log == nil {
		log := logger.New(
			logger.Conf{
				Debug: conf.Debug,
			})
		f.log = log
	} else {
		f.log = conf.Log
	}
	f.log.Debug("Debug logging enabled for fetcher")

	return f, nil
}

func (f *fetcher) fetchHTTP(u *url.URL) (<-chan string, error) {
	ch := make(chan string, 1024) // TODO make adjustable?

	resp, err := http.Get(u.String())
	if err != nil {
		f.log.Error("Could not GET list from %q", u.String())
		return nil, err
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/plain;") {
		f.log.Error("Unknown content type %q in response", contentType)
		return nil, common.ErrNotCompleted
	}

	go func() {
		scanner := bufio.NewScanner(resp.Body)
		defer resp.Body.Close()
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "#") { // TODO make configurable?
				continue
			}

			ch <- line
		}

		err := scanner.Err()
		if err != nil {
			f.log.Error("Encountered error after scanning fetched file: %s", err)
		}

		close(ch)
	}()

	return ch, nil
}

func (f *fetcher) fetchFILE(u *url.URL) (<-chan string, error) {
	panic("not impl")
	return nil, nil
}

func (f *fetcher) fetchTEST(u *url.URL) (<-chan string, error) {
	ch := make(chan string, 1024)

	file, err := testLists.Open(strings.TrimPrefix(u.Path, "/"))
	if err != nil {
		f.log.Error("Could not open test list")
		return nil, err
	}

	go func() {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "#") { // TODO make configurable?
				continue
			}

			ch <- line
		}

		err := scanner.Err()
		if err != nil {
			f.log.Error("Encountered error after scanning test file: %s", err)
		}

		close(ch)
	}()

	return ch, nil
}

func (f *fetcher) Fetch(u string) (<-chan string, error) {
	parsedURL, err := url.Parse(u)
	if err != nil {
		f.log.Error("Failed to parse URL %q: %v", u, err)
		return nil, common.ErrBadParam
	}

	switch parsedURL.Scheme {
	case "http":
		return f.fetchHTTP(parsedURL)
	case "https":
		return f.fetchHTTP(parsedURL)
	case "go":
		return f.fetchTEST(parsedURL)
	case "file":
		return f.fetchFILE(parsedURL)
	default:
		f.log.Error("Unrecognized scheme '%s' for fetcher", parsedURL.Scheme)
		return nil, common.ErrBadParam
	}
}
