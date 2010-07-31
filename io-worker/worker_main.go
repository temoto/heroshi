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
    "syscall"
)

// TODO: make it configurable
const NumConcurrentRequests = 1000

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

    go processSignals()
    go stdinReader()

    // In terms of Eventlet, this is a TokenPool.
    // processUrl reserves an item from this channel,
    // which limits max concurrent requests.
    limiter := make(chan bool, NumConcurrentRequests)
    for i := 1; i <= NumConcurrentRequests; i++ {
        limiter <- true
    }

    processUrl := func(item conc.Box) {
        <-limiter

        url := item.(*http.URL)
        result := worker.Fetch(url)
        result_json, err := json.Marshal(result)
        if err != nil {
            panic("JSON encode")
            return
        }
        println(string(result_json))

        limiter <- true
    }

    wait := conc.For(urls, processUrl)
    wait()
}
