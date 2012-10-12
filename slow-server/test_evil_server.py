# coding: utf-8
import random
import socket
import time


DELAY = 0.2 # in seconds

RESPONSE_200_EMPTY = """HTTP/1.0 200 OK\r\n\
Connection: Close\r\n\
Content-Length: 2\r\n\
\r\n\
42"""


def slow_respond(client):
    req = client.recv(4*1024)
    print "--- received:", req
    time.sleep(DELAY)
    for ch in RESPONSE_200_EMPTY:
        #print "--- sending", ch
        client.send(ch)
        time.sleep(DELAY)
    client.shutdown(socket.SHUT_RDWR)

def main():
    listen_addr = ('', 8000)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(listen_addr)
    srv.listen(10)

    print "--- Listening for incoming connections on", listen_addr, "evil delay is", DELAY

    while True:
        client, _ = srv.accept()
        print "--- Client connected."
        try:
            slow_respond(client)
        except IOError, e:
            if e.errno == 32: # EPIPE
                print "--- Haha, EPIPE!"
            else:
                print "***", unicode(e)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
