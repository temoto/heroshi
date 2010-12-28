package main

import (
    "bufio"
    "bytes"
    "fmt"
    "goconc.googlecode.com/hg"
    "http"
    "json"
    "os"
    "os/signal"
    "strconv"
    "syscall"
)

const DefaultConcurrency = 1000

var urls chan conc.Box


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


func main() {
    worker := newWorker()
    urls = make(chan conc.Box)

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
        }
    }

    go processSignals()
    go stdinReader()

    // In terms of Eventlet, this is a TokenPool.
    // processUrl reserves an item from this channel,
    // which limits max concurrent requests.
    limiter := make(chan bool, max_concurrency)
    for i := uint(1); i <= max_concurrency; i++ {
        limiter <- true
    }

    processUrl := func(item conc.Box) {
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
        }

        <-limiter

        url := item.(*http.URL)
        result := worker.Fetch(url)

        report.url = result.Url
        report.success = result.Success
        report.status = result.Status
        report.status_code = result.StatusCode
        report.headers = result.Headers
        report.content = result.Body
        report.cached = result.Cached

        report_json, err := json.Marshal(report)
        if err != nil {
            panic("JSON encode")
            return
        }
        println(string(report_json))

        limiter <- true
    }

    wait := conc.For(urls, processUrl)
    wait()
}
