package heroshi

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type ConnectionHandler func(*testing.T, net.Conn)

type SlowReaderWriter struct {
	R      io.Reader
	W      io.Writer
	Size   int
	RDelay time.Duration
	WDelay time.Duration
}

func (s *SlowReaderWriter) Read(p []byte) (int, error) {
	time.Sleep(s.RDelay)
	size := len(p)
	if size > s.Size {
		size = s.Size
	}
	return s.R.Read(p[0:size])
}

func (s *SlowReaderWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		time.Sleep(s.WDelay)
		size := s.Size
		if size > len(p) {
			size = len(p)
		}
		n, err := s.W.Write(p[0:size])
		total += n
		if err != nil {
			return total, err
		}
		p = p[n:len(p)]
	}
	return total, nil
}

func (s *SlowReaderWriter) Close() error {
	return nil
}

func makeServe(connectionClose bool, processTime time.Duration, bodyLength int, slow *SlowReaderWriter) ConnectionHandler {
	return func(t *testing.T, conn net.Conn) {
		defer conn.Close()

		var br *bufio.Reader = bufio.NewReader(conn)
		var w io.Writer = conn
		body := strings.Repeat("x", bodyLength)
		if slow != nil {
			slow.R = conn
			slow.W = conn
			br = bufio.NewReader(slow)
			w = io.Writer(slow)
		}

		for i := 1; i < 10; i++ {
			request, err := http.ReadRequest(br)
			if err != nil {
				t.Error("Read:", err.Error())
			}

			response := http.Response{
				Status:     "200 OK",
				StatusCode: 200,
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				// No RDelay, used only to provide Close method to strings.Reader.
				Body:          &SlowReaderWriter{R: strings.NewReader(body), Size: bodyLength},
				ContentLength: int64(bodyLength),
				Close:         connectionClose,
				Header:        make(http.Header),
				Request:       request,
			}
			response.Header.Set("Content-Type", "text/plain")
			response.Header.Set("Content-Length", fmt.Sprintf("%d", bodyLength))

			// dw := bytes.NewBuffer(make([]byte, 100000))
			// response.Write(dw)
			// println("Write:", dw.String())

			err = response.Write(w)
			if err != nil {
				t.Error("Write:", err.Error())
			}
		}
		t.Fatal("Too many requests on one connection")
	}
}

func server(t *testing.T, listener net.Listener, connHandler ConnectionHandler, stopCh chan bool, acceptDelay time.Duration) {
	defer listener.Close()

	connCh := make(chan net.Conn, 0)
	errCh := make(chan error, 0)
	go func() {
		for {
			time.Sleep(acceptDelay)
			conn, err := listener.Accept()
			if err == nil {
				connCh <- conn
			} else {
				errCh <- err
			}
		}
	}()
AcceptLoop:
	for {
		select {
		case <-stopCh:
			break AcceptLoop
		case conn := <-connCh:
			go connHandler(t, conn)
		case err := <-errCh:
			if _, ok := <-stopCh; !ok {
				t.Error("Accept:", err.Error())
			} else {
				break AcceptLoop
			}
		}
	}
}

func TestConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, makeServe(true, 0, 1, nil), stopCh, 10*time.Millisecond)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-connect", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			ConnectTimeout: 5 * time.Millisecond,
		}
		response, err := transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		} else {
			response.Body.Close()
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(50 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestReadTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, makeServe(true, 5*time.Millisecond, 1, nil), stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-respond", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			ReadTimeout: 5 * time.Millisecond,
		}
		_, err = transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
				return
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(100 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestWriteTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	slow := &SlowReaderWriter{
		Size:   1024,
		RDelay: 10 * time.Millisecond,
	}
	go server(t, listener, makeServe(true, 0, 500, slow), stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/slow-receive", listener.Addr().String())
	request, err := http.NewRequest("POST", url, strings.NewReader(strings.Repeat("garbage890", 100000)))
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	ready := make(chan bool, 0)
	go func() {
		transport := &Transport{}
		options := &RequestOptions{
			WriteTimeout: 5 * time.Millisecond,
		}
		response, err := transport.RoundTripOptions(request, options)
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				t.Log("Expected transport timeout:", neterr.Error())
			} else if err != nil {
				t.Error("Client error:", err.Error())
			}
		} else {
			response.Body.Close()
		}
		ready <- true
	}()
	select {
	case <-ready:
	case <-time.After(100 * time.Millisecond):
		t.Error("Transport did not timeout")
	}
}

func TestClose(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, makeServe(true, 0, 42, nil), stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/delay", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	transport := &Transport{}
	conn, err := transport.GetConnRequest(request, nil)
	if err != nil {
		t.Fatal("GetConnRequest:", err.Error())
	}
	err = conn.WriteRequest(request, nil)
	if err != nil {
		t.Fatal("WriteRequest:", err.Error())
	}

	ch := make(chan *http.Response, 0)
	go func() {
		response, err := conn.ReadResponse(nil)
		if err != nil {
			t.Error("ReadResponse:", err.Error())
			ch <- nil
		} else {
			ch <- response
		}
	}()
	select {
	case <-ch:
	case <-time.After(10 * time.Millisecond):
		t.Log("Expected outer timeout, close")
		if err = conn.Close(); err != nil {
			t.Fatal("Close:", err.Error())
		}
	}

	// Check if this transport can be used to make new request to same server.
	response, err := transport.RoundTripOptions(request, nil)
	if err != nil {
		t.Fatal("Second RoundTrip:", err.Error())
	}
	if response.StatusCode != 200 {
		t.Fatal("Status is not 200")
	}
}

func TestReadLimit01(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, makeServe(true, 0, 4000, nil), stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/big", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	transport := &Transport{}

	buf := new(bytes.Buffer)
	response, err := transport.RoundTripOptions(request, nil)
	if err != nil {
		t.Fatal("RoundTrip:", err.Error())
	}
	response.Header.Write(buf)
	_, err = io.Copy(buf, response.Body)
	if err != nil {
		t.Fatal("Without ReadLimit: copy Body:", err.Error())
	}
	if buf.Len() < 100 {
		t.Fatal("Without ReadLimit: expected more than 100 bytes")
	}

	buf = new(bytes.Buffer)
	response, err = transport.RoundTripOptions(request, &RequestOptions{ReadLimit: 100})
	if err != nil {
		t.Fatal("RoundTrip:", err.Error())
	}
	response.Header.Write(buf)
	_, err = io.Copy(buf, response.Body)
	if err != nil {
		t.Fatal("Without ReadLimit: copy Body:", err.Error())
	}
	if buf.Len() > 100 {
		t.Fatal("With ReadLimit: limit exceeded")
	}
}

// ReadLimit must count from 0 for each new request.
func TestReadLimitFalseNegative(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal("Listen:", err.Error())
	}
	stopCh := make(chan bool, 1)
	go server(t, listener, makeServe(false, 0, 700, nil), stopCh, 0)
	defer func() { stopCh <- true }()

	url := fmt.Sprintf("http://%s/big", listener.Addr().String())
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal("NewRequest:", err.Error())
	}

	transport := &Transport{}
	options := &RequestOptions{ReadLimit: 1000}
	buf := make([]byte, options.ReadLimit)

	for i := 1; i <= 5; i++ {
		options.Stat = &RequestStat{}
		response, err := transport.RoundTripOptions(request, options)
		if err != nil {
			t.Fatal("RoundTrip:", err.Error())
		}
		if response.StatusCode != 200 {
			t.Fatal("RoundTrip: status!=200")
		}
		n, err := response.Body.Read(buf)
		response.Body.Close()
		if n == len(buf) {
			t.Fatal("The buf is not enough!")
		}
		if err != nil {
			t.Fatal("Body.Read:", err.Error())
		}
		if options.Stat.ConnectionUse != uint(i) {
			t.Fatal("Loop:", i, "ConnectionUse:", options.Stat.ConnectionUse)
		}
	}
}
