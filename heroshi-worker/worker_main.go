package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"github.com/temoto/heroshi/heroshi"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"
)

type Nothing struct{}

var urls chan *url.URL
var reports chan []byte

func stdinReader(stop chan bool) {
	defer func() { stop <- true }()

	var line string
	var u *url.URL
	var err error
	stdinReader := bufio.NewReader(os.Stdin)
	for {
		lineBytes, readErr := stdinReader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			panic("At ReadBytes")
			return
		}

		lineBytes = bytes.TrimSpace(lineBytes)
		if len(lineBytes) == 0 {
			goto Next
		}
		line = string(lineBytes)

		u, err = url.Parse(line)
		if err != nil {
			u = &url.URL{
				Host: line,
			}
			result := heroshi.ErrorResult(u, err.Error())
			reportJson, _ := encodeResult(line, result)
			reports <- reportJson
		} else {
			urls <- u
		}

	Next:
		if readErr == io.EOF {
			return
		}
	}
}

func encodeResult(key string, result *heroshi.FetchResult) (encoded []byte, err error) {
	// Copy of FetchResult struct with new field Key and base64-encoded Body.
	// This is ugly and violates DRY principle.
	// But also, it allows to extract fetcher as separate package.
	var report struct {
		Key        string              `json:"key"`
		Url        string              `json:"url"`
		Success    bool                `json:"success"`
		Status     string              `json:"status"`
		StatusCode int                 `json:"status_code"`
		Headers    map[string][]string `json:"headers,omitempty"`
		Content    string              `json:"content,omitempty"`
		Length     int64               `json:"length,omitempty"`
		Cached     bool                `json:"cached"`
		FetchTime  uint                `json:"fetch_time,omitempty"`
		TotalTime  uint                `json:"total_time,omitempty"`
		// new
		RemoteAddr     string `json:"address,omitempty"`
		Started        string `json:"started"`
		ConnectionAge  uint   `json:"connection_age"`
		ConnectionUse  uint   `json:"connection_use"`
		ResolveTime    uint   `json:"resolve_time"`
		ConnectTime    uint   `json:"connect_time"`
		WriteTime      uint   `json:"write_time,omitempty"`
		ReadHeaderTime uint   `json:"read_header_time,omitempty"`
		ReadBodyTime   uint   `json:"read_body_time,omitempty"`
	}
	report.Key = key
	report.Url = result.Url.String()
	report.Success = result.Success
	report.Status = result.Status
	report.StatusCode = result.StatusCode
	report.Headers = result.Headers
	report.Cached = result.Cached
	report.FetchTime = result.FetchTime
	report.TotalTime = result.TotalTime
	contentEncoded := make([]byte, base64.StdEncoding.EncodedLen(len(result.Body)))
	base64.StdEncoding.Encode(contentEncoded, result.Body)
	report.Content = string(contentEncoded)
	report.Length = result.Length
	// new
	if result.Stat != nil {
		if result.Stat.RemoteAddr != nil {
			report.RemoteAddr = result.Stat.RemoteAddr.String()
		}
		report.Started = result.Stat.Started.UTC().Format(time.RFC3339)
		report.ConnectionAge = uint(result.Stat.ConnectionAge / time.Millisecond)
		report.ConnectionUse = result.Stat.ConnectionUse
		report.ConnectTime = uint(result.Stat.ConnectTime / time.Millisecond)
		report.WriteTime = uint(result.Stat.WriteTime / time.Millisecond)
		report.ReadHeaderTime = uint(result.Stat.ReadHeaderTime / time.Millisecond)
		report.ReadBodyTime = uint(result.Stat.ReadBodyTime / time.Millisecond)
	}

	encoded, err = json.Marshal(report)
	if err != nil {
		encoded = nil
		log.Printf("Url: %s, error encoding report: %s\n",
			result.Url, err.Error())

		// Most encoding errors happen in content. Try to recover.
		report.Content = ""
		report.Status = err.Error()
		report.Success = false
		report.StatusCode = 0
		encoded, err = json.Marshal(report)
		if err != nil {
			encoded = nil
			log.Printf("Url: %s, error encoding recovery report: %s\n",
				result.Url, err.Error())
		}
	}
	return
}

func reportWriter(done chan bool) {
	for r := range reports {
		if r != nil {
			os.Stdout.Write(r)
			os.Stdout.Write([]byte("\n"))
		}
	}
	done <- true
}

func main() {
	worker := newWorker()
	urls = make(chan *url.URL)

	// Process command line arguments.
	var maxConcurrency uint
	flag.UintVar(&maxConcurrency, "jobs", 1000, "Try to crawl this many URLs in parallel.")
	flag.UintVar(&worker.HostConcurrency, "host-jobs", 1, "Per-host concurrency. RFC2616 tells it SHOULD NOT be > 2.")
	flag.UintVar(&worker.FollowRedirects, "redirects", 10, "How many redirects to follow. Can be 0.")
	flag.BoolVar(&worker.SkipRobots, "skip-robots", false, "Don't request and obey robots.txt.")
	flag.BoolVar(&worker.SkipBody, "skip-body", false, "Don't return response body in results.")
	flag.DurationVar(&worker.ConnectTimeout, "connect-timeout", 15*time.Second, "Timeout to query DNS and establish TCP connection.")
	flag.DurationVar(&worker.FetchTimeout, "total-timeout", 60*time.Second, "Total timeout for crawling one URL. Includes all network IO, fetching and checking robots.txt.")
	flag.DurationVar(&worker.IOTimeout, "io-timeout", 30*time.Second, "Timeout for sending request and receiving response (applied for each, so total time is twice this timeout).")
	flag.DurationVar(&worker.KeepaliveTimeout, "keepalive-timeout", 120*time.Second, "Timeout for keeping persistent connections to servers since last operation.")
	flag.Uint64Var(&worker.ReadLimit, "read-limit", DefaultReadLimit, "Limit size of response (including headers and body) in bytes.")
	flag.StringVar(&worker.UserAgent, "user-agent", DefaultUserAgent, "User-Agent header. It is highly recommended to replace unknown_owner with your contact email.")
	showHelp := flag.Bool("help", false, "")
	cpuprofile := flag.String("cpuprofile", "", "Write CPU profile to file")
	memprofile := flag.String("memprofile", "", "Write memory profile to file")

	flag.Parse()
	if maxConcurrency <= 0 {
		log.Println("Invalid concurrency limit:", maxConcurrency)
		os.Exit(1)
	}
	if *showHelp {
		os.Stderr.WriteString(`HTTP client.
Reads URLs on stdin, fetches them and writes results as JSON on stdout.

Follows up to 10 redirects.
Fetches /robots.txt first and obeys rules there using first word of User-Agent to test against rules.

Try 'echo http://localhost/ |heroshi-worker' to see sample of result JSON.

Run 'heroshi-worker -h' for flags description.
`)
		os.Exit(1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		defer f.Close()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
		go func() {
			for {
				time.Sleep(5 * time.Second)
				pprof.WriteHeapProfile(f)
			}
		}()
		defer pprof.WriteHeapProfile(f)
		defer f.Close()
	}

	// Set number of parallel threads to number of CPUs.
	runtime.GOMAXPROCS(runtime.NumCPU())

	reports = make(chan []byte, maxConcurrency)
	stop := make(chan bool)
	doneWriting := make(chan bool)

	sigIntChan := make(chan os.Signal, 1)
	signal.Notify(sigIntChan, syscall.SIGINT)
	go func() {
		<-sigIntChan
		log.Println("Waiting for remaining requests to complete.")
		stop <- true
	}()

	go stdinReader(stop)
	go reportWriter(doneWriting)

	limit := make(chan Nothing, maxConcurrency)
	var urlCount uint64 = 0
	busy := sync.WaitGroup{}

	processUrl := func(url *url.URL) {
		result := worker.Fetch(url)
		reportJson, _ := encodeResult(url.String(), result)

		// nil report is really unrecoverable error. Check stderr.
		if reportJson != nil {
			reports <- reportJson
		}

		busy.Done()
		<-limit
	}

readUrlsLoop:
	for {
		select {
		case u, ok := <-urls:
			if !ok {
				break readUrlsLoop
			}
			limit <- Nothing{}
			urlCount++
			busy.Add(1)
			go processUrl(u)

			if urlCount%20 == 0 {
				nHosts, nConns := worker.hostLimits.Size()
				println("--- URL #", urlCount, "Open", nConns, "connections to", nHosts, "hosts.")
			}
		case <-stop:
			close(urls)
			break readUrlsLoop
		}
	}

	var prevUrlCount uint64
	go func() {
		const statusPrintDuration time.Duration = 1 * time.Second
		for {
			prevUrlCount = urlCount
			time.Sleep(statusPrintDuration)
			speed := float64(urlCount-prevUrlCount) / float64(statusPrintDuration/time.Second)
			nHosts, nConns := worker.hostLimits.Size()
			println("--- URL #", urlCount, "Open", nConns, "connections to", nHosts, "hosts. Speed:", int(speed*10)/10, "qps")
			runtime.GC()
		}
	}()

	busy.Wait()
	close(reports)
	<-doneWriting
}
