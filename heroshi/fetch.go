package heroshi

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type FetchResult struct {
	Url        *url.URL
	Success    bool
	Status     string
	StatusCode int
	Headers    http.Header
	Body       []byte
	Length     int64
	Cached     bool
	FetchTime  uint
	TotalTime  uint
	Stat       *RequestStat
}

func ErrorResult(url *url.URL, reason string) *FetchResult {
	return &FetchResult{
		Url:     url,
		Success: false,
		Status:  reason,
	}
}

func BeginFetch(transport *Transport, req *http.Request, options *RequestOptions, ch chan *FetchResult) io.Closer {
	// debug
	if false {
		dump, err := httputil.DumpRequest(req, true)
		if err != nil {
			panic(err)
		}
		println(string(dump))
	}

	conn, err := transport.GetConnRequest(req, options)
	if err != nil {
		ch <- ErrorResult(req.URL, err.Error())
		return nil
	}

	go func() {
		err := conn.WriteRequest(req, options)
		if err != nil {
			ch <- ErrorResult(req.URL, err.Error())
			return
		}
		response, err := conn.ReadResponse(options)
		if err != nil {
			ch <- ErrorResult(req.URL, err.Error())
			return
		}

		var read_body_started time.Time
		if options != nil && options.Stat != nil {
			read_body_started = time.Now()
		}

		defer response.Body.Close()
		var buf bytes.Buffer
		var body_len int64
		body_len, err = io.Copy(&buf, response.Body)

		if options != nil && options.Stat != nil {
			options.Stat.ReadBodyTime = time.Now().Sub(read_body_started)
		}

		responseBody := buf.Bytes()
		if err != nil {
			ch <- ErrorResult(req.URL, err.Error())
			return
		}

		ch <- &FetchResult{
			Url:        req.URL,
			Success:    true,
			Status:     response.Status,
			StatusCode: response.StatusCode,
			Body:       responseBody,
			Length:     body_len,
			Headers:    response.Header,
		}
	}()

	return conn
}

func Fetch(transport *Transport, req *http.Request, options *RequestOptions, timeout time.Duration) (result *FetchResult) {
	if options != nil && options.Stat != nil && options.Stat.Started.IsZero() {
		options.Stat.Started = time.Now()
	}

	ch := make(chan *FetchResult, 1)
	conn := BeginFetch(transport, req, options, ch)

	select {
	case result = <-ch:
	case <-time.After(timeout):
		// TODO: check result of Close
		_ = conn.Close()
		result = ErrorResult(req.URL, fmt.Sprintf("Fetch timeout: %d", timeout/time.Millisecond))
	}

	if options != nil && options.Stat != nil && !options.Stat.Started.IsZero() {
		options.Stat.TotalTime = time.Now().Sub(options.Stat.Started)
		result.FetchTime = uint(options.Stat.TotalTime / time.Millisecond)
	}

	return result
}
