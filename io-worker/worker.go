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

    // How many redirects to follow. Default is 1.
    FollowRedirects uint

    // Timeout for single socket read or write.
    // In nanoseconds. Default is 1e9 (1 second). 0 disables timeout.
    IOTimeout uint64

    // Timeout for whole download. This includes establishing connection,
    // sending request, receiving response.
    // In nanoseconds. Default is 60e9 (1 minute). DO NOT use 0.
    FetchTimeout uint64

    // How long to keep persistent connections, in seconds. Default is 60.
    KeepAlive uint

    // Keep-alive HTTP clients.
    clients map[string]*Client
    // clients list lock
    cl_lk *sync.Mutex

    cache redis.Client
}


func newWorker() *Worker {
    return &Worker{
        FollowRedirects: 1,
        IOTimeout:       1e9,
        FetchTimeout:    60e9,
        KeepAlive:       60,
        clients:         make(map[string]*Client, 1000),
        cl_lk:           new(sync.Mutex),
    }
}

// Deletes `http.Client` associated with `authority` after `timeout`.
// TODO: prolong timeout on consequtive requests.
func (w *Worker) staleClient(authority string, timeout uint) {
    time.Sleep(int64(timeout) * 1e9)
    w.cl_lk.Lock()
    w.clients[authority].Close()
    w.clients[authority] = nil, false
    w.cl_lk.Unlock()
}

// Downloads url and returns whatever result was.
// This function WILL NOT follow redirects.
func (w *Worker) Download(url *http.URL) (result *FetchResult) {
    w.cl_lk.Lock()
    client := w.clients[url.Host]
    is_new_client := client == nil
    if client == nil {
        client = new(Client)
        client.IOTimeout = w.IOTimeout
        w.clients[url.Host] = client
    }
    w.cl_lk.Unlock()

    req := new(http.Request)
    req.URL = url
    req.Header = make(map[string]string, 10)
    req.UserAgent = "HeroshiBot/0.3 (+http://temoto.github.com/heroshi/; temotor@gmail.com)"

    result = client.FetchWithTimeout(req, w.FetchTimeout)

    if is_new_client {
        go w.staleClient(url.Host, w.KeepAlive)
    }

    return result
}

func (w *Worker) CacheOrDownload(url *http.URL) *FetchResult {
    if encoded, err := w.cache.Get(url.Raw); err == nil {
        cached := new(FetchResult)
        if err := json.Unmarshal(encoded, cached); err == nil {
            cached.Cached = true
            return cached
        }
    }
    result := w.Download(url)
    return result
}

func (w *Worker) Fetch(url *http.URL) (result *FetchResult) {

    original_url := *url
    started := time.Nanoseconds()

    for redirect := uint(0); ; redirect++ {
        if redirect > w.FollowRedirects {
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
                result = ErrorResult(original_url.Raw, err.String())
                break
            }
            continue
        }

        // no redirects required
        break
    }
    ended := time.Nanoseconds()
    result.TotalTime = uint( (ended - started) / 1e6 ) // in milliseconds

    if !result.Cached {
        encoded, _ := json.Marshal(result)
        w.cache.Set(original_url.Raw, encoded)
    }
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
    // TODO: Try to decode body using appropriate character encoding.
    // string() only tries UTF-8. This is great, but not always sufficient.
    body_string := string(fetch_result.Body)
    robots, err = robotstxt.FromResponse(fetch_result.StatusCode, body_string, false)
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

    if !allow {
        return allow, ErrorResult(url.Raw, "Robots disallow")
    }

    return allow, nil
}
