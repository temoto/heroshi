// This file is originally copied from Go src/pkg/net/http/transport.go
// Differences:
// * per-request timeouts on single socket operations
// * method to abort a request (by closing its connection)
// * proxy support is removed for simplicity
//   (many required types/methods are not exported from net/http)
// * transparent gzip decompression is removed for simplicity

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// HTTP client implementation. See RFC 2616.
//
// This is the low-level Transport implementation of RoundTripper.
// The high-level interface is in fetch.go.

package heroshi

import (
	"bufio"
	"crypto/tls"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DefaultMaxIdleConnsPerHost is the default value of Transport's
// MaxIdleConnsPerHost.
const DefaultMaxIdleConnsPerHost = 2

// Transport is an implementation of RoundTripper that supports http and https.
// Does not support proxies.
// Will cache connections for future re-use.
type Transport struct {
	lk       sync.Mutex
	idleConn map[string][]*PersistConn

	// TODO: tunable on global max cached connections
	// TODO: tunable on timeout on cached connections
	// TODO: optional pipelining

	// Dial specifies the dial function for creating TCP connections.
	// If Dial is nil, net.Dial is used.
	Dial func(net, addr string, opt *RequestOptions) (c net.Conn, err error)

	// TLSClientConfig specifies the TLS configuration to use with
	// tls.Client. If nil, the default configuration is used.
	TLSClientConfig *tls.Config

	// MaxIdleConnsPerHost, if non-zero, controls the maximum idle
	// (keep-alive) to keep to keep per-host.  If zero,
	// DefaultMaxIdleConnsPerHost is used.
	MaxIdleConnsPerHost int
}

type RequestOptions struct {
	ConnectTimeout   time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	ReadLimit        uint64
	KeepaliveTimeout time.Duration
	Stat             *RequestStat
}

type RequestStat struct {
	RemoteAddr     net.Addr
	Started        time.Time
	ConnectionAge  time.Duration
	ConnectionUse  uint
	ConnectTime    time.Duration
	WriteTime      time.Duration
	ReadHeaderTime time.Duration
	// These are not yet filled by this transport. But outer code
	// may fill these fields to have all in one place.
	ReadBodyTime time.Duration
	TotalTime    time.Duration
}

type Error struct {
	str       string
	timeout   bool
	temporary bool
}

// Implements error and net.Error
func (e *Error) Error() string   { return e.str }
func (e *Error) Timeout() bool   { return e.timeout }
func (e *Error) Temporary() bool { return e.temporary }

// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func HasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

func (t *Transport) RoundTripOptions(req *http.Request, opt *RequestOptions) (resp *http.Response, err error) {
	if opt != nil && opt.Stat != nil && opt.Stat.Started.IsZero() {
		opt.Stat.Started = time.Now()
	}

	if req.URL == nil {
		return nil, &Error{str: "http: nil Request.URL"}
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return nil, &Error{str: "unsupported protocol scheme: " + req.URL.Scheme}
	}

	// Get the cached or newly-created connection to the host (for http or https).
	// In any case, we'll be ready to send it requests.
	pconn, err := t.GetConnRequest(req, opt)
	if err != nil {
		return nil, err
	}

	err = pconn.WriteRequest(req, opt)
	if err != nil {
		return nil, err
	}
	return pconn.ReadResponse(opt)
}

// RoundTrip implements the RoundTripper interface.
func (t *Transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	return t.RoundTripOptions(req, nil)
}

// CloseIdleConnections closes any connections which were previously
// connected from previous requests but are now sitting idle in
// a "keep-alive" state. It does not interrupt any connections currently
// in use.
// If force is true, all keep alive connections are closed regardless of when
// they were established or last used. If force is false, a connection will be
// closed only after .keepalive duration from last usage.
func (t *Transport) CloseIdleConnections(force bool) {
	t.lk.Lock()
	defer t.lk.Unlock()
	if t.idleConn == nil {
		return
	}

	var now time.Time
	if !force {
		now = time.Now()
	}
	for _, conns := range t.idleConn {
		for _, pconn := range conns {
			// Already closed (broken) will be closed again, assume that's not a problem.
			if force || now.Sub(pconn.lastUsed) > pconn.idleTimeout {
				pconn.Close()
			}
		}
	}
	t.idleConn = make(map[string][]*PersistConn)
}

//
// Private implementation past this point.
//

func (t *Transport) ConnectMethodForRequest(req *http.Request) (*ConnectMethod, error) {
	cm := &ConnectMethod{
		targetScheme: req.URL.Scheme,
		targetAddr:   canonicalAddr(req.URL),
	}
	return cm, nil
}

// putIdleConn adds pconn to the list of idle persistent connections awaiting
// a new request.
// If pconn is no longer needed or not in a good state, putIdleConn
// returns false.
func (t *Transport) putIdleConn(pconn *PersistConn) bool {
	if t.MaxIdleConnsPerHost < 0 {
		pconn.Close()
		return false
	}
	if pconn.isBroken() {
		return false
	}

	key := pconn.cacheKey
	max := t.MaxIdleConnsPerHost
	if max == 0 {
		max = DefaultMaxIdleConnsPerHost
	}

	t.lk.Lock()
	defer t.lk.Unlock()
	if len(t.idleConn[key]) >= max {
		pconn.Close()
		return false
	}
	t.idleConn[key] = append(t.idleConn[key], pconn)
	return true
}

func (t *Transport) getIdleConn(cm *ConnectMethod) (pconn *PersistConn) {
	t.lk.Lock()
	defer t.lk.Unlock()
	if t.idleConn == nil {
		t.idleConn = make(map[string][]*PersistConn)
		return nil
	}
	key := cm.String()
	for {
		pconns, ok := t.idleConn[key]
		if !ok {
			return nil
		}
		if len(pconns) == 1 {
			pconn = pconns[0]
			delete(t.idleConn, key)
		} else {
			// 2 or more cached connections; pop last
			// TODO: queue?
			pconn = pconns[len(pconns)-1]
			t.idleConn[key] = pconns[0 : len(pconns)-1]
		}
		if !pconn.isBroken() {
			return
		}
	}
	return
}

func (t *Transport) dial(network, addr string, opt *RequestOptions) (c net.Conn, err error) {
	if t.Dial != nil {
		c, err = t.Dial(network, addr, opt)
	} else if opt != nil && opt.ConnectTimeout != 0 {
		c, err = net.DialTimeout(network, addr, opt.ConnectTimeout)
	} else {
		c, err = net.Dial(network, addr)
	}

	// Custom Dial may have provide these values, do not overwrite.
	if err == nil && opt != nil && opt.Stat != nil && opt.Stat.RemoteAddr == nil {
		opt.Stat.RemoteAddr = c.RemoteAddr()
		opt.Stat.ConnectionAge = 0
		opt.Stat.ConnectionUse = 1
	}
	return
}

func (t *Transport) GetConnRequest(req *http.Request, opt *RequestOptions) (*PersistConn, error) {
	cm, err := t.ConnectMethodForRequest(req)
	if err != nil {
		return nil, err
	}
	return t.GetConn(cm, opt)
}

// GetConn dials and creates a new PersistConn to the target specified in the ConnectMethod.
// This includes setting up TLS.
// If this doesn't return an error, the PersistConn is ready to write requests to.
func (t *Transport) GetConn(cm *ConnectMethod, opt *RequestOptions) (*PersistConn, error) {
	if pc := t.getIdleConn(cm); pc != nil {
		pc.useCount++
		if opt != nil && opt.Stat != nil {
			opt.Stat.RemoteAddr = pc.conn.RemoteAddr()
			opt.Stat.ConnectionAge = time.Now().Sub(pc.started)
			opt.Stat.ConnectionUse = pc.useCount
		}
		return pc, nil
	}

	conn, err := t.dial("tcp", cm.addr(), opt)
	if err != nil {
		return nil, err
	}

	pconn := &PersistConn{
		cacheKey:    cm.String(),
		conn:        conn,
		reqch:       make(chan requestAndOptions, 50),
		rech:        make(chan responseAndError, 1),
		started:     time.Now(),
		useCount:    1,
		idleTimeout: 120 * time.Second,
	}
	if opt != nil && opt.KeepaliveTimeout != 0 {
		pconn.idleTimeout = opt.KeepaliveTimeout
	}

	if cm.targetScheme == "https" {
		// Initiate TLS and check remote host name against certificate.
		conn = tls.Client(conn, t.TLSClientConfig)
		if err = conn.(*tls.Conn).Handshake(); err != nil {
			return nil, err
		}
		if t.TLSClientConfig == nil || !t.TLSClientConfig.InsecureSkipVerify {
			if err = conn.(*tls.Conn).VerifyHostname(cm.tlsHost()); err != nil {
				return nil, err
			}
		}
		pconn.conn = conn
	}

	pconn.bw = bufio.NewWriter(pconn.conn)
	go pconn.readLoop(func(pc *PersistConn) bool { return t.putIdleConn(pc) })
	return pconn, nil
}

// ConnectMethod is the map key (in its String form) for keeping persistent
// TCP connections alive for subsequent HTTP requests.
//
// A connect method may be of the following types:
//
// Cache key form                Description
// -----------------             -------------------------
// http|foo.com                  http directly to server
// https|foo.com                 https directly to server
//
type ConnectMethod struct {
	targetScheme string // "http" or "https"
	targetAddr   string
}

func (cm *ConnectMethod) String() string {
	return strings.Join([]string{cm.targetScheme, cm.targetAddr}, "|")
}

// addr returns the first hop "host:port" to which we need to TCP connect.
func (cm *ConnectMethod) addr() string {
	return cm.targetAddr
}

// tlsHost returns the host name to match against the peer's
// TLS certificate.
func (cm *ConnectMethod) tlsHost() string {
	h := cm.targetAddr
	if HasPort(h) {
		h = h[:strings.LastIndex(h, ":")]
	}
	return h
}

// PersistConn wraps a connection.
// WriteRequest/ReadResponse are not concurrent-safe.
type PersistConn struct {
	cacheKey    string // its ConnectMethod.String()
	conn        net.Conn
	bw          *bufio.Writer          // to conn
	reqch       chan requestAndOptions // written by WriteRequest(); read by readLoop()
	rech        chan responseAndError  // read by ReadResponse
	started     time.Time
	lastUsed    time.Time
	connectTime time.Duration
	idleTimeout time.Duration
	useCount    uint

	lk                   sync.Mutex // guards numExpectedResponses and broken
	numExpectedResponses int
	broken               bool // an error has happened on this connection; marked broken so it's not reused.
}

func (pc *PersistConn) isBroken() bool {
	pc.lk.Lock()
	defer pc.lk.Unlock()
	return pc.broken
}

var remoteSideClosedFunc func(error) bool // or nil to use default

func remoteSideClosed(err error) bool {
	if err == io.EOF {
		return true
	}
	if remoteSideClosedFunc != nil {
		return remoteSideClosedFunc(err)
	}
	return false
}

func (pc *PersistConn) readLoop(putIdleConn func(*PersistConn) bool) {
	alive := true
	var lastbody io.ReadCloser // last response body, if any, read on this connection

	for alive {
		limitedReader := &io.LimitedReader{R: pc.conn, N: 1}
		br := bufio.NewReader(limitedReader)

		pb, err := br.Peek(1)

		pc.lk.Lock()
		if pc.numExpectedResponses == 0 {
			err = pc.closeLocked()
			// TODO: return this error to caller
			// pc.rech <- responseAndError{nil, err}
			// But who is reading that channel without first making a request?
			pc.lk.Unlock()
			if len(pb) > 0 {
				// TODO: return this error to caller
				// pc.rech <- responseAndError{nil, err}
				// But who is reading that channel without first making a request?
				log.Printf("Unsolicited response received on idle HTTP channel starting with %q; err=%v",
					string(pb), err)
				pc.Close()
			}
			return
		}
		pc.lk.Unlock()

		rc := <-pc.reqch

		// Advance past the previous response's body, if the
		// caller hasn't done so.
		if lastbody != nil {
			lastbody.Close() // assumed idempotent
			lastbody = nil
		}

		// Set read limit, if any.
		if rc.opt != nil && rc.opt.ReadLimit != 0 {
			limitedReader.N = int64(rc.opt.ReadLimit)
		} else {
			limitedReader.N = 1<<63 - 1
		}

		// Separate started variable because pc.lastUsed may be updated concurrently.
		var started time.Time = time.Now()
		pc.lastUsed = started

		var resp *http.Response
		if rc.opt == nil || rc.opt.ReadTimeout == 0 {
			resp, err = http.ReadResponse(br, rc.req)
		} else {
			ch := make(chan responseAndError, 0)
			go func() {
				r, e := http.ReadResponse(br, rc.req)
				ch <- responseAndError{r, e}
			}()
			select {
			case re := <-ch:
				resp, err = re.resp, re.err
			case <-time.After(rc.opt.ReadTimeout):
				resp, err = nil, &Error{str: "ReadResponse timeout", timeout: true, temporary: true}
			}
		}

		if rc.opt != nil && rc.opt.Stat != nil {
			rc.opt.Stat.ReadHeaderTime = time.Now().Sub(started)
		}

		if err != nil {
			pc.Close()
		} else {
			resp.Body = &bodyEOFSignal{body: resp.Body}
		}

		if err != nil || resp.Close || rc.req.Close {
			alive = false
		}

		hasBody := resp != nil && resp.ContentLength != 0
		var waitForBodyRead chan bool
		if alive {
			if hasBody {
				lastbody = resp.Body
				waitForBodyRead = make(chan bool)
				resp.Body.(*bodyEOFSignal).fn = func() {
					if !putIdleConn(pc) {
						alive = false
					}
					waitForBodyRead <- true
				}
			} else {
				// When there's no response body, we immediately
				// reuse the TCP connection (putIdleConn), but
				// we need to prevent ClientConn.Read from
				// closing the Response.Body on the next
				// loop, otherwise it might close the body
				// before the client code has had a chance to
				// read it (even though it'll just be 0, EOF).
				lastbody = nil

				if !putIdleConn(pc) {
					alive = false
				}
			}
		}

		pc.rech <- responseAndError{resp, err}

		// Wait for the just-returned response body to be fully consumed
		// before we race and peek on the underlying bufio reader.
		if waitForBodyRead != nil {
			<-waitForBodyRead
		}
	}
}

type responseAndError struct {
	resp *http.Response
	err  error
}

type requestAndOptions struct {
	req *http.Request
	opt *RequestOptions
}

func (pc *PersistConn) WriteRequest(req *http.Request, opt *RequestOptions) (err error) {
	pc.lk.Lock()
	pc.numExpectedResponses++
	pc.lk.Unlock()

	// Separate started variable because pc.lastUsed may be updated concurrently.
	var started time.Time = time.Now()
	pc.lastUsed = started

	if opt == nil || opt.WriteTimeout == 0 {
		err = req.Write(pc.bw)
	} else {
		ch := make(chan error, 0)
		go func() {
			ch <- req.Write(pc.bw)
		}()
		select {
		case err = <-ch:
		case <-time.After(opt.WriteTimeout):
			err = &Error{str: "WriteRequest timeout", timeout: true, temporary: true}
		}
	}
	if opt != nil && opt.Stat != nil {
		opt.Stat.WriteTime = time.Now().Sub(started)
	}
	if err != nil {
		pc.Close()
		return
	}
	pc.bw.Flush()

	pc.reqch <- requestAndOptions{req, opt}

	return err
}

func (pc *PersistConn) ReadResponse(options *RequestOptions) (resp *http.Response, err error) {
	re := <-pc.rech
	pc.lk.Lock()
	pc.numExpectedResponses--
	pc.lk.Unlock()
	return re.resp, re.err
}

func (pc *PersistConn) Close() error {
	pc.lk.Lock()
	defer pc.lk.Unlock()
	return pc.closeLocked()
}

func (pc *PersistConn) closeLocked() error {
	pc.broken = true
	return pc.conn.Close()
}

var portMap = map[string]string{
	"http":  "80",
	"https": "443",
}

// canonicalAddr returns url.Host but always with a ":port" suffix
func canonicalAddr(url *url.URL) string {
	addr := url.Host
	if !HasPort(addr) {
		return addr + ":" + portMap[url.Scheme]
	}
	return addr
}

// bodyEOFSignal wraps a ReadCloser but runs fn (if non-nil) at most
// once, right before the final Read() or Close() call returns, but after
// EOF has been seen.
type bodyEOFSignal struct {
	body     io.ReadCloser
	fn       func()
	isClosed bool
}

func (es *bodyEOFSignal) Read(p []byte) (n int, err error) {
	n, err = es.body.Read(p)
	if es.isClosed && n > 0 {
		panic("http: unexpected bodyEOFSignal Read after Close; see issue 1725")
	}
	if err == io.EOF && es.fn != nil {
		es.fn()
		es.fn = nil
	}
	return
}

func (es *bodyEOFSignal) Close() (err error) {
	if es.isClosed {
		return nil
	}
	es.isClosed = true
	err = es.body.Close()
	if err == nil && es.fn != nil {
		es.fn()
		es.fn = nil
	}
	return
}

type readFirstCloseBoth struct {
	io.ReadCloser
	io.Closer
}

func (r *readFirstCloseBoth) Close() error {
	if err := r.ReadCloser.Close(); err != nil {
		r.Closer.Close()
		return err
	}
	if err := r.Closer.Close(); err != nil {
		return err
	}
	return nil
}

// discardOnCloseReadCloser consumes all its input on Close.
type discardOnCloseReadCloser struct {
	io.ReadCloser
}

func (d *discardOnCloseReadCloser) Close() error {
	io.Copy(ioutil.Discard, d.ReadCloser) // ignore errors; likely invalid or already closed
	return d.ReadCloser.Close()
}
