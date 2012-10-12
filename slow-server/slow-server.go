package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"net/http"
	"time"
)

type ConnectionHandler func(net.Conn)

func delayServe(conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReader(conn)
	_, err := http.ReadRequest(br)
	if err != nil {
		log.Println("delay:Read:", err.Error())
	}

	time.Sleep(responseDelay)
	response := []byte("HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: 19\r\n\r\nEverything is fine.")
	n, err := conn.Write(response)
	if err != nil {
		log.Println("delay:Write:", err.Error())
	}
	if n < len(response) {
		log.Println("fast:delay: written", n, "bytes of", len(response))
	}
}

func server(listener net.Listener, connHandler ConnectionHandler) {
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
	for {
		select {
		case conn := <-connCh:
			go connHandler(conn)
		case err := <-errCh:
			log.Fatalln("Accept:", err.Error())
		}
	}
}

var acceptDelay, responseDelay time.Duration

func main() {
	addrString := flag.String("bind", ":0", "Listen [addr]:port")
	flag.DurationVar(&acceptDelay, "accept-delay", 100*time.Millisecond, "")
	flag.DurationVar(&responseDelay, "response-delay", 100*time.Millisecond, "")
	flag.Parse()

	listener, err := net.Listen("tcp", *addrString)
	if err != nil {
		log.Fatal("Listen:", err.Error())
	}
	log.Printf("Listen on http://%s/", listener.Addr())
	server(listener, delayServe)
}
