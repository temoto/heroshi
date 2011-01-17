package main

import (
    "bufio"
    "bytes"
    "encoding/base64"
    "flag"
    "fmt"
    "http"
    "json"
    "os"
    "os/signal"
    "syscall"
    "time"
)

// Should be same as heroshi.__init__.TIME_FORMAT.
const HeroshiTimeFormat = "2006-01-02T15:04:05"
const DefaultConcurrency = 1000

var urls chan *http.URL
var reports chan []byte


func processSignals() {
    defer close(urls)

    for sig := range signal.Incoming {
        if unix_sig := sig.(signal.UnixSignal); unix_sig == syscall.SIGINT {
            fmt.Fprintln(os.Stderr, "Waiting for remaining requests to complete.")
            return
        }
    }
}

func stdinReader() {
    defer close(urls)

    stdinReader := bufio.NewReader(os.Stdin)
    for {
        line, read_err := stdinReader.ReadBytes('\n')
        if read_err != nil && read_err != os.EOF {
            panic("At ReadBytes")
            return
        }

        line = bytes.TrimSpace(line)
        if len(line) == 0 {
            goto Next
        }

        url, err := http.ParseURLReference(string(line))
        if err != nil {
            result := ErrorResult(string(line), err.String())
            report_json, _ := encodeResult(result)
            reports <- report_json
        } else {
            urls <- url
        }

    Next:
        if read_err == os.EOF {
            return
        }
    }
}


func encodeResult(result *FetchResult) (encoded []byte, err os.Error) {
    // Copy of FetchResult struct with lowercase names.
    // This is ugly and violates DRY principle.
    // But also, it allows to extract fetcher as separate package.
    var report struct {
        Url         string "url"
        Success     bool   "success"
        Status      string "status"
        Status_code int    "status_code"
        Headers     map[string]string "headers"
        Content     string "content"
        Cached      bool   "cached"
        Visited     string "visited"
        Fetch_time  uint   "fetch_time"
        Total_time  uint   "total_time"
    }
    report.Url = result.Url
    report.Success = result.Success
    report.Status = result.Status
    report.Status_code = result.StatusCode
    report.Headers = result.Headers
    report.Cached = result.Cached
    report.Visited = time.UTC().Format(HeroshiTimeFormat)
    report.Fetch_time = result.FetchTime
    report.Total_time = result.TotalTime
    content_encoded := make([]byte, base64.StdEncoding.EncodedLen(len(result.Body)))
    base64.StdEncoding.Encode(content_encoded, result.Body)
    report.Content = string(content_encoded)

    encoded, err = json.Marshal(report)
    if err != nil {
        encoded = nil
        fmt.Fprintf(os.Stderr, "Url: %s, error encoding report: %s\n",
            result.Url, err.String())

        // Most encoding errors happen in content. Try to recover.
        report.Content = ""
        report.Status = err.String()
        report.Success = false
        report.Status_code = 0
        encoded, err = json.Marshal(report)
        if err != nil {
            encoded = nil
            fmt.Fprintf(os.Stderr, "Url: %s, error encoding recovery report: %s\n",
                result.Url, err.String())
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
    urls = make(chan *http.URL)

    // Process command line arguments.
    var max_concurrency uint
    flag.UintVar(&max_concurrency,        "jobs",          DefaultConcurrency, "Try to crawl this many URLs in parallel.")
    flag.UintVar(&worker.FollowRedirects, "redirects",     10,    "How many redirects to follow. Can be 0.")
    flag.UintVar(&worker.KeepAlive,       "keepalive",     120,   "Keep persistent connections to servers for this many seconds.")
    flag.BoolVar(&worker.SkipRobots,      "skip-robots",   false, "Don't request and obey robots.txt.")
    flag.Uint64Var(&worker.IOTimeout,     "io-timeout",    30e3,  "Timeout for single socket operation (read, write) in milliseconds.")
    flag.Uint64Var(&worker.FetchTimeout,  "total-timeout", 60e3,  "Total timeout for crawling one URL, in milliseconds. Includes all network IO, fetching and checking robots.txt.")
    show_help := flag.Bool("help", false, "")
    flag.Parse()
    if max_concurrency <= 0 {
        fmt.Fprintln(os.Stderr, "Invalid concurrency limit:", max_concurrency)
        os.Exit(1)
    }
    if *show_help {
        fmt.Fprint(os.Stderr, `Heroshi IO worker.
Reads URLs on stdin, fetches them and writes results as JSON on stdout.

By default, follows up to 10 redirects.
By default, fetches /robots.txt first and obeys rules there.

Try 'echo http://localhost/ | io-worker' to see sample of result JSON.
`)
        os.Exit(1)
    }
    // Milliseconds to nanoseconds.
    worker.IOTimeout *= 1e6
    worker.FetchTimeout *= 1e6

    reports = make(chan []byte, max_concurrency)
    done_writing := make(chan bool)

    go processSignals()
    go stdinReader()
    go reportWriter(done_writing)

    limit := make(chan bool, max_concurrency)
    busy_count := make(chan uint, 1)

    busyCountGet := func() uint {
        n := <-busy_count
        busy_count <- n
        return n
    }

    processUrl := func(url *http.URL) {
        result := worker.Fetch(url)
        report_json, _ := encodeResult(result)

        // nil report is really unrecoverable error. Check stderr.
        if report_json != nil {
            reports <- report_json
        }

        busy_count <- (<-busy_count - 1) // atomic decrement
        <-limit
    }

    busy_count <- 0
    for url := range urls {
        limit <- true
        busy_count <- (<-busy_count + 1) // atomic decrement
        go processUrl(url)
    }

    // Ugly poll until all urls are processed.
    for n := busyCountGet(); n > 0; n = busyCountGet() {
        time.Sleep(100 * 1e6) // in milliseconds
    }
    close(reports)
    <-done_writing
}
