package main

import (
    "encoding/base64"
    "fmt"
    "http"
    "io"
    "io/ioutil"
    "net"
    "os"
    "strings"
    "sync"
)

// Copied from httplib.go
type Client struct {
    conn    *http.ClientConn
    lk      sync.Mutex
    LastURL *http.URL
}

type FetchResult struct {
    Url        string
    Success    bool
    Status     string
    StatusCode int
    Headers    map[string]string
    Body       string
    Cached     bool
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
// Used in Send to implement io.ReadCloser by bundling together the
// io.BufReader through which we read the response, and the underlying
// network connection.
type readClose struct {
    io.Reader
    io.Closer
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

// Mostly copy of unexported http.send
func (client *Client) Request(req *http.Request) (resp *http.Response, err os.Error) {
    if req.URL.Scheme != "http" {
        return nil, os.NewError(fmt.Sprint("unsupported protocol scheme", req.URL.Scheme))
    }

    if client.conn == nil || client.LastURL == nil ||
        client.LastURL.Host != req.URL.Host {
        //
        addr := req.URL.Host
        if !hasPort(addr) {
            addr += ":http"
        }

        creds := req.URL.RawUserinfo
        if len(creds) > 0 {
            enc := base64.URLEncoding
            encoded := make([]byte, enc.EncodedLen(len(creds)))
            enc.Encode(encoded, []byte(creds))
            if req.Header == nil {
                req.Header = make(map[string]string)
            }
            req.Header["Authorization"] = "Basic " + string(encoded)
        }

        var tcpConn net.Conn
        if tcpConn, err = net.Dial("tcp", "", addr); err != nil {
            return nil, err
        }
        client.conn = http.NewClientConn(tcpConn, nil)
    }

    err = client.conn.Write(req)
    if err != nil {
        client.conn.Close()
        return nil, err
    }

    resp, err = client.conn.Read()
    if err != nil && resp == nil {
        client.conn.Close()
        return nil, err
    }

    return resp, nil
}

func (client *Client) Get(url *http.URL) (result *FetchResult) {
    //
    req := new(http.Request)
    req.URL = url
    req.Header = make(map[string]string, 10)
    req.UserAgent = "HeroshiBot/0.3 (+http://temoto.github.com/heroshi/; temotor@gmail.com)"

    // debug
    if false {
        dump, _ := http.DumpRequest(req, true)
        print(string(dump))
    }

    // Prevent simultaneous IO from different goroutines.
    client.lk.Lock()
    defer client.lk.Unlock()

    resp, err := client.Request(req)
    if err != nil {
        return ErrorResult(url.Raw, err.String())
    }

    b, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return ErrorResult(url.Raw, err.String())
    }

    responseBody := string(b)
    resp.Body.Close()

    return &FetchResult{
        Url:        url.Raw,
        Success:    true,
        Status:     resp.Status,
        StatusCode: resp.StatusCode,
        Body:       responseBody,
        Headers:    resp.Header,
    }
}
