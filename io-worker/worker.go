package main

import (
    "fmt"
    "http"
    "json"
    "os"
    "redis"
    "robotstxt"
    "sync"
    "time"
)


type Worker struct {
    // When false (default), worker will obey /robots.txt
    // when true, any URL is allowed to visit.
    SkipRobots bool

    // Keep-alive HTTP clients.
    clients map[string]*Client
    // clients list lock
    cl_lk *sync.Mutex

    cache redis.Client
}


func newWorker() *Worker {
    return &Worker{
        clients: make(map[string]*Client, 1000),
        cl_lk:   new(sync.Mutex),
    }
}

// Deletes `http.Client` associates with `authority` after `timeout`.
func (w *Worker) staleClient(authority string, timeout int) {
    time.Sleep(int64(timeout) * 1e9)
    w.cl_lk.Lock()
    w.clients[authority] = nil, false
    w.cl_lk.Unlock()
}

// Downloads url and returns whatever result was.
// This function WILL NOT follow redirects.
func (w *Worker) Download(url *http.URL) *FetchResult {
    w.cl_lk.Lock()
    client := w.clients[url.Authority]
    if client == nil {
        client = new(Client)
        w.clients[url.Authority] = client
        go w.staleClient(url.Authority, 120)
    }
    w.cl_lk.Unlock()

    result := client.Get(url)

    return result
}

func (w *Worker) CacheOrDownload(url *http.URL) *FetchResult {
    if encoded, err := w.cache.Get(url.Raw); err == nil {
        cached := new(FetchResult)
        if err := json.Unmarshal(encoded, cached); err == nil {
            return cached
        }
    }
    result := w.Download(url)
    return result
}

func (w *Worker) Fetch(url *http.URL) (result *FetchResult) {

    for redirect := 0; ; redirect++ {
        if redirect > 10 {
            result = ErrorResult(url.Raw, "Too much redirects")
            break
        }

        if url.Scheme == "" || url.Host == "" {
            result = ErrorResult(url.Raw, "Incorrect URL: "+url.Raw)
            break
        }

        // The /robots.txt is always allowed, check others.
        if w.SkipRobots || url.Path == "/robots.txt" {
        } else {
            var allow bool
            allow, result = w.AskRobots(url)
            if !allow {
                break
            }
        }

        result = w.CacheOrDownload(url)
        if ShouldRedirect(result.StatusCode) {
            location := result.Headers["Location"]
            var err os.Error
            url, err = http.ParseURL(location)
            if err != nil {
                result = ErrorResult(url.Raw, err.String())
                break
            }
            continue
        }

        // no redirects required
        break
    }
    encoded, _ := json.Marshal(result)
    w.cache.Set(url.Raw, encoded)
    return result
}

func (w *Worker) AskRobots(url *http.URL) (bool, *FetchResult) {
    robots_url_str := fmt.Sprintf("%s://%s/robots.txt", url.Scheme, url.Host)
    robots_url, err := http.ParseURL(robots_url_str)
    if err != nil {
        return false, ErrorResult(url.Raw, err.String())
    }

    fetch_result := w.Fetch(robots_url)

    if !fetch_result.Success {
        fetch_result.Status = "Robots download error: " + fetch_result.Status
        return false, fetch_result
    }

    var robots *robotstxt.RobotsData
    robots, err = robotstxt.FromResponse(fetch_result.StatusCode, fetch_result.Body)
    if err != nil {
        fetch_result.Status = "Robots parse error: " + err.String()
        return false, fetch_result
    }

    robots.DefaultAgent = "HeroshiBot"

    var allow bool
    allow, err = robots.Test(url.RawPath)
    if err != nil {
        return false, ErrorResult(url.Raw, "Robots test error: "+err.String())
    }

    return allow, nil
}
