package main

import (
    "bufio"
    "bytes"
    "fmt"
    "http"
    "json"
    "os"
    "os/signal"
    "strconv"
    "syscall"
    "time"
)

// Should be same as heroshi.__init__.TIME_FORMAT.
const HeroshiTimeFormat = "2006-01-02T15:04:05"
const DefaultConcurrency = 1000

var urls chan *http.URL


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
            panic("ParseURL: " + err.String())
            return
        }
        urls <- url

    Next:
        if read_err == os.EOF {
            return
        }
    }
}

func help_str(self_name string) string {
    return fmt.Sprint(`Heroshi IO worker.
Reads URLs on stdin, fetches them and writes results as JSON on stdout.

Usage:
    ` + self_name + ` [skip-robots]

When called w/o arguments, IO worker tries to fetch /robots.txt first,
    parse it and proceed with original request if it considers allow.

With ` + "`skip-robots`" + ` option, IO worker doesn't care about /robots.txt.
    It behaves like a parallel curl.

Currently there's a hardcoded limit on total concurrent connections: 1000.

Try ` + "`echo http://localhost/ | " + self_name + "`" + ` to see sample of result JSON.
`)
}


func encodeResult(result *FetchResult) (encoded []byte, err os.Error) {
    // Copy of FetchResult struct with lowercase names.
    // This is ugly and violates DRY principle.
    // But also, it allows to extract fetcher as separate package.
    var report struct {
        url         string
        success     bool
        status      string
        status_code int
        headers     map[string]string
        content     string
        cached      bool
        visited     string
        fetch_time  uint
    }
    report.url = result.Url
    report.success = result.Success
    report.status = result.Status
    report.status_code = result.StatusCode
    report.headers = result.Headers
    report.content = result.Body
    report.cached = result.Cached
    report.visited = time.UTC().Format(HeroshiTimeFormat)
    report.fetch_time = result.TotalTime

    encoded, err = json.Marshal(report)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Url: %s, error encoding report: %s\n",
            result.Url, err.String())

        // Most encoding errors happen in content. Try to recover.
        report.content = ""
        report.status = err.String()
        report.success = false
        report.status_code = 0
        encoded, err = json.Marshal(report)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Url: %s, error encoding recovery report: %s\n",
                result.Url, err.String())
        }
    }
    return
}

func main() {
    worker := newWorker()
    urls = make(chan *http.URL)

    // Process command line arguments.
    for _, s := range os.Args[1:] {
        switch s {
        case "help", "-help", "--help":
            print(help_str(os.Args[0]))
            os.Exit(1)
        case "skip-robots":
            worker.SkipRobots = true
        }
    }

    max_concurrency := uint(DefaultConcurrency)
    env_concurrency := os.Getenv("HEROSHI_IO_CONCURRENCY")
    if env_concurrency != "" {
        x, err := strconv.Atoui(env_concurrency)
        if err == nil {
            max_concurrency = x
        } else {
            fmt.Fprintln(os.Stderr, "Invalid concurrency limit:", err.String())
        }
    }

    if max_concurrency <= 0 {
        fmt.Fprintln(os.Stderr, "Invalid concurrency limit:", max_concurrency)
    }

    go processSignals()
    go stdinReader()

    // In terms of Eventlet, this is a TokenPool.
    // processUrl reserves an item from this channel,
    // which limits max concurrent requests.
    busy := make(chan bool, max_concurrency)
    done := make(chan bool)
    url_count := 0

    // Report writer
    reports := make(chan []byte)
    go func() {
        for report := range reports {
            // nil report is really unrecoverable error. Check stderr.
            if report != nil {
                os.Stdout.Write(report)
                os.Stdout.Write([]byte("\n"))
            }
            done <- true
        }
    }()

    // Fetcher
    processUrl := func(url) {
        result := worker.Fetch(url)

        report_json, err := encodeResult(result)
        if err == nil {
            reports <- report_json
        } else {
            reports <- nil
        }

        <-busy
    }
    for url := range urls {
        busy <- true
        url_count++
        go processUrl(url)
    }

    // Wait until all URLs are processed.
    for done_count := 0; done_count < url_count; {
        <-done
        done_count++
    }
}
