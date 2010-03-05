from eventlet import listen, sleep, wsgi
import random
import webob


def server(request):
    response = webob.Response()
    pause = 1 + random.random() * 5
    sleep(pause)
    response.body = "Slept well for %f sec." % (pause,)
    return response

def wsgi_app(environ, start_response):
    request = webob.Request(environ)
    response = server(request)
    return response(environ, start_response)

def main():
    sock = listen( ('127.0.0.1', 9293) )
    wsgi.server(sock, wsgi_app)

if __name__ == "__main__": main()
