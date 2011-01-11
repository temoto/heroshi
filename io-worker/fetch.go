package main

import (
    "bytes"
    "encoding/base64"
    "fmt"
    "http"
    "io"
    "net"
    "os"
    "strings"
    "sync"
    "time"
)

// Copied from httplib.go
type Client struct {
    HttpConn *http.ClientConn
    NetConn  *net.TCPConn
    LastURL  *http.URL

    // Timeout for single IO socket read or write.
    // Fetch consists of many IO operations. 30 seconds is reasonable, but
    // in some cases more could be required.
    // In nanoseconds. Default is 0 - no timeout.
    IOTimeout uint64

    // Fetch lock. Prevents simultaneous `FetchWithTimeout` calls for proper
    // timeout accounting.
    // TODO: find a better way to implement it. Without a separate lock?
    fetch_lk sync.Mutex

    // Internal IO operations lock.
    lk       sync.Mutex
}

type FetchResult struct {
    Url        string
    Success    bool
    Status     string
    StatusCode int
    Headers    map[string]string
    Body       []byte
    Cached     bool
    FetchTime  uint
    TotalTime  uint
}

func ErrorResult(url, reason string) *FetchResult {
    return &FetchResult{
        Url:     url,
        Success: false,
        Status:  reason,
    }
}

// Copied from $(GOROOT)/src/pkg/http/client.go
//
// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

// Copied from $(GOROOT)/src/pkg/http/client.go
//
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

func (client *Client) Connect(addr string) (err os.Error) {
    if !hasPort(addr) {
        addr += ":http"
    }

    var tcp_addr *net.TCPAddr
    tcp_addr, err = net.ResolveTCPAddr(addr)
    if err != nil {
        return err
    }

    client.NetConn, err = net.DialTCP("tcp", nil, tcp_addr)
    if err != nil {
        return err
    }
    client.NetConn.SetKeepAlive(true)
    client.NetConn.SetLinger(0)
    // NoDelay is true by default, that's what we need
    client.NetConn.SetTimeout(int64(client.IOTimeout))

    client.HttpConn = http.NewClientConn(client.NetConn, nil)
    return nil
}

func (client *Client) Abort() (err os.Error) {
    err = client.NetConn.Close()
    client.Close()
    return err
}

func (client *Client) Close() (err os.Error) {
    if client.HttpConn != nil {
        // Read buffer returned by HttpConn.Close may contain left over data.
        client.HttpConn.Close()
        client.HttpConn = nil
    }
    err = client.NetConn.Close()
    client.NetConn = nil
    return err
}

func (client *Client) SendRequest(req *http.Request) (err os.Error) {
    if req.URL.Scheme != "http" {
        return os.NewError(fmt.Sprint("unsupported protocol scheme", req.URL.Scheme))
    }

    // Prevent simultaneous IO from different goroutines.
    client.lk.Lock()
    defer client.lk.Unlock()

    if client.HttpConn == nil || client.LastURL == nil ||
        client.LastURL.Host != req.URL.Host {
        //
        if err = client.Connect(req.URL.Host); err != nil {
            return err
        }
    }

    err = client.HttpConn.Write(req)
    if err != nil {
        client.Close()
        return err
    }

    return nil
}

func (client *Client) GetResponse() (resp *http.Response, err os.Error) {
    // Prevent simultaneous IO from different goroutines.
    client.lk.Lock()
    defer client.lk.Unlock()

    resp, err = client.HttpConn.Read()
    if err != nil && resp == nil {
        client.Close()
        return nil, err
    }

    return resp, nil
}

// Shortcut for synchronous SendRequest + GetResponse.
// Use those two functions if you want pipelining.
func (client *Client) Request(req *http.Request) (resp *http.Response, err os.Error) {
    err = client.SendRequest(req)
    if err != nil { return nil, err }
    resp, err = client.GetResponse()
    return
}

func (client *Client) Fetch(req *http.Request) (result *FetchResult) {
    creds := req.URL.RawUserinfo
    _, has_auth_header := req.Header["Authorization"]
    if len(creds) > 0 && !has_auth_header {
        encoded := make([]byte, base64.URLEncoding.EncodedLen(len(creds)))
        base64.URLEncoding.Encode(encoded, []byte(creds))
        if req.Header == nil {
            req.Header = make(map[string]string)
        }
        req.Header["Authorization"] = "Basic " + string(encoded)
    }

    // debug
    if false {
        dump, _ := http.DumpRequest(req, true)
        print(string(dump))
    }

    resp, err := client.Request(req)
    if err != nil {
        return ErrorResult(req.URL.Raw, err.String())
    }

    // Prevent simultaneous IO from different goroutines.
    client.lk.Lock()
    defer client.lk.Unlock()

    var buf bytes.Buffer
    _, err = io.Copy(&buf, resp.Body)
    responseBody := buf.Bytes()
    if err != nil {
        return ErrorResult(req.URL.Raw, err.String())
    }
    resp.Body.Close()

    return &FetchResult{
        Url:        req.URL.Raw,
        Success:    true,
        Status:     resp.Status,
        StatusCode: resp.StatusCode,
        Body:       responseBody,
        Headers:    resp.Header,
    }
}

func (client *Client) FetchWithTimeout(req *http.Request, limit uint64) (result *FetchResult) {
    client.fetch_lk.Lock()
    defer client.fetch_lk.Unlock()

    rch := make(chan *FetchResult)
    timeout := time.After(int64(limit))
    started := time.Nanoseconds()
    go func() {
        rch <- client.Fetch(req)
    }()

    select {
    case result = <-rch:
    case <-timeout:
        client.Abort()
        result = ErrorResult(req.URL.Raw, fmt.Sprintf("Fetch timeout: %d", limit / 1e6))
    }
    ended := time.Nanoseconds()
    result.FetchTime = uint( (ended - started) / 1e6 ) // in milliseconds

    return result
}
