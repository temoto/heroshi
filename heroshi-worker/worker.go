package main

import (
	"errors"
	"fmt"
	"github.com/temoto/heroshi/heroshi"
	"github.com/temoto/heroshi/limitmap" // Temporary location
	"github.com/temoto/robotstxt.go"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
	// Cache:
	//"github.com/hoisie/redis.go"
	//"json"
)

const DefaultUserAgent = "HeroshiBot/1 (unknown_owner; +http://temoto.github.com/heroshi/)"
const DefaultReadLimit = 10 << 20 // 10MB

type Worker struct {
	// When false (default), worker will obey /robots.txt
	// when true, any URL is allowed to visit.
	SkipRobots bool

	// When false (default) worker will fetch and return response body
	// when true response body will be discarded after received.
	SkipBody bool

	// How many redirects to follow. Default is 1.
	FollowRedirects uint

	// Timeout to resolve domain name (if needed) and establish TCP.
	// Default is 1 second. 0 disables timeout.
	ConnectTimeout time.Duration

	// Timeout for single socket read or write.
	// Default is 1 second. 0 disables timeout.
	IOTimeout time.Duration

	// Timeout for whole download. This includes establishing connection,
	// sending request, receiving response.
	// Default is 1 minute. DO NOT use 0.
	FetchTimeout time.Duration

	ReadLimit uint64

	// How long to keep persistent connections. Default is 60 seconds.
	KeepaliveTimeout time.Duration

	// Maximum number of connections per domain:port pair. Default is 1.
	HostConcurrency uint

	// User-Agent as it's sent to server
	// robotsAgent (first word of UserAgent) is verified against robots.txt.
	UserAgent   string
	robotsAgent string

	//cache redis.Client
	hostLimits *limitmap.LimitMap
	transport  *heroshi.Transport
}

func newWorker() *Worker {
	w := &Worker{
		FollowRedirects:  1,
		ConnectTimeout:   1 * time.Second,
		IOTimeout:        1 * time.Second,
		FetchTimeout:     60 * time.Second,
		ReadLimit:        DefaultReadLimit,
		KeepaliveTimeout: 60 * time.Second,
		HostConcurrency:  1,
		UserAgent:        DefaultUserAgent,
		hostLimits:       limitmap.NewLimitMap(),
		transport: &heroshi.Transport{
			Dial:                Dial,
			MaxIdleConnsPerHost: 1,
		},
	}
	w.robotsAgent = FirstWord(w.UserAgent)
	return w
}

// Downloads url and returns whatever result was.
// This function WILL NOT follow redirects.
func (w *Worker) Download(url *url.URL) (result *heroshi.FetchResult) {
	w.hostLimits.Acquire(url.Host, w.HostConcurrency)
	defer w.hostLimits.Release(url.Host)

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return heroshi.ErrorResult(url, err.Error())
	}
	req.Header.Set("User-Agent", w.UserAgent)

	options := &heroshi.RequestOptions{
		ConnectTimeout:   w.ConnectTimeout,
		ReadTimeout:      w.IOTimeout,
		WriteTimeout:     w.IOTimeout,
		ReadLimit:        w.ReadLimit,
		KeepaliveTimeout: w.KeepaliveTimeout,
		Stat:             new(heroshi.RequestStat),
	}
	result = heroshi.Fetch(w.transport, req, options, w.FetchTimeout)
	if w.SkipBody {
		result.Body = nil
	}
	result.Stat = options.Stat
	w.transport.CloseIdleConnections(false)

	return result
}

/*
func (w *Worker) CacheOrDownload(url *url.URL) *FetchResult {
    key := url.String()

    if encoded, err := w.cache.Get(key); err == nil {
        cached := new(FetchResult)
        if err := json.Unmarshal(encoded, cached); err == nil {
            cached.Cached = true
            return cached
        }
    }
    result := w.Download(url)
    encoded, _ := json.Marshal(result)
    w.cache.Set(key, encoded)
    return result
}
*/

func (w *Worker) Fetch(url *url.URL) (result *heroshi.FetchResult) {
	originalUrl := url
	started := time.Now()
	defer func() {
		if result != nil {
			ended := time.Now()
			result.TotalTime = uint((ended.Sub(started)) / time.Millisecond)
		}
	}()

	for redirect := uint(0); redirect <= w.FollowRedirects; redirect++ {
		if url.Scheme == "" || url.Host == "" {
			return heroshi.ErrorResult(url, "Incorrect URL: "+url.String())
		}

		// The /robots.txt is always allowed, check others.
		if w.SkipRobots || url.Path == "/robots.txt" {
		} else {
			var allow bool
			allow, result = w.AskRobots(url)
			if !allow {
				return result
			}
		}

		//result = w.CacheOrDownload(url)
		result = w.Download(url)
		if ShouldRedirect(result.StatusCode) {
			location := result.Headers.Get("Location")
			var err error
			url, err = url.Parse(location)
			if err != nil {
				return heroshi.ErrorResult(originalUrl, err.Error())
			}
			continue
		}

		// no redirects required
		return result
	}
	return result
}

func (w *Worker) AskRobots(url *url.URL) (bool, *heroshi.FetchResult) {
	robotsUrlString := fmt.Sprintf("%s://%s/robots.txt", url.Scheme, url.Host)
	robotsUrl, err := url.Parse(robotsUrlString)
	if err != nil {
		return false, heroshi.ErrorResult(url, err.Error())
	}

	fetchResult := w.Fetch(robotsUrl)

	if !fetchResult.Success {
		fetchResult.Status = "Robots download error: " + fetchResult.Status
		return false, fetchResult
	}

	var robots *robotstxt.RobotsData
	robots, err = robotstxt.FromStatusAndBytes(fetchResult.StatusCode, fetchResult.Body)
	if err != nil {
		fetchResult.Status = "Robots parse error: " + err.Error()
		return false, fetchResult
	}

	allow := robots.TestAgent(url.Path, w.UserAgent)
	if !allow {
		return allow, heroshi.ErrorResult(url, "Robots disallow")
	}

	return allow, nil
}

func Dial(netw, addr string, options *heroshi.RequestOptions) (net.Conn, error) {
	// TODO: resolve time limit
	// tcpAddr, err := net.ResolveTCPAddr(netw, addr)
	// log.Println("Dial: resolved:", tcpAddr)
	// if err != nil {
	// 	return nil, err
	// }
	var tcpAddr net.TCPAddr
	addrString, _ := heroshi.SplitPort(addr)
	addrs, resolveTime, err := heroshi.ResolveName(addrString, "127.0.0.1:53")
	if err != nil {
		log.Println("Dial: resolve error:", err.Error(), "in", resolveTime.String())
		return nil, err
	}
	if len(addrs) > 0 {
		ip := addrs[0]
		tcpAddr.IP = ip
		log.Println("Dial: resolved:", ip.String(), "in", resolveTime.String())
	}
	addr = tcpAddr.String()
	if options != nil && options.Stat != nil {
		options.Stat.RemoteAddr = &tcpAddr
	}

	return nil, errors.New("Dial-stub")

	var conn net.Conn
	if options != nil && options.ConnectTimeout != 0 {
		conn, err = net.DialTimeout(netw, addr, options.ConnectTimeout)
	} else {
		conn, err = net.Dial(netw, addr)
	}
	if err != nil {
		return conn, err
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return conn, errors.New("Dial: conn->TCPConn type assertion failed.")
	}
	tcpConn.SetKeepAlive(true)
	tcpConn.SetLinger(0)
	tcpConn.SetNoDelay(true)
	return tcpConn, err
}

func FirstWord(s string) string {
	i := strings.IndexFunc(s, func(r rune) bool { return !unicode.IsLetter(r) })
	if i != -1 {
		return s[0:i]
	}
	return s
}

// True if the specified HTTP status code is one for which the Get utility should
// automatically redirect.
func ShouldRedirect(statusCode int) bool {
	switch statusCode {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect:
		//
		return true
	}
	return false
}
