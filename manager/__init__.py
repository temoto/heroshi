# -*- coding: utf-8 -*-

import os, sys, time
import random
from optparse import OptionParser
import cPickle as pickle
import cjson as json
import wsgiref.simple_server
import urllib

import protocol
import misc
from misc import HEROSHI_VERSION, debug

os_path_expand = lambda p: os.path.expandvars(os.path.expanduser(p))

QUEUE_PATH = os_path_expand('~/.heroshi/queue')
MANAGER_SOCKET = os_path_expand('~/.heroshi/manager.sock')
WORKER_BUFFER = 10

crawl_queue = None

class CrawlQueue(object):
    """Persistent list"""
    # TODO: rewrite to shelve

    queue = []
    store_path = None

    def __init__(self, store_path):
        self.store_path = store_path

    def load(self):
        if not os.path.exists(self.store_path):
            return
        f = open(self.store_path, 'rb')
        self.queue = pickle.load(f)
        f.close()
        debug("Loaded queue of %d items." % len(self.queue))

    def save(self):
        path_dir = os.path.dirname(self.store_path)
        if not os.path.isdir(path_dir):
            os.makedirs(path_dir)
        f = open(self.store_path, 'wb')
        pickle.dump(self.queue, f)
        f.close()


class WSGIServer(wsgiref.simple_server.WSGIServer):
    is_alive = False

    def __init__(self, server_address, handler_class):
        self.server_address = server_address
        self.handler_class = handler_class

    def init_socket(self):
        self.is_alive = True
        # TODO: ohshi, socket is binding in __init__. Let them die
        wsgiref.simple_server.WSGIServer.__init__(self, self.server_address, self.handler_class)


class Request(object):
    def __str__(self):
        attr_list = [ (attr, getattr(self, attr)) for attr in dir(self) if not attr.startswith('_') ]
        return "\n".join([ "%s: %s" % attr for attr in attr_list ])


def make_response_ok(data):
    return {'status': 'ok', 'data': data}

def make_response_failure(message):
    return {'status': 'failure', 'data': message}

def s_get(data, environ):
    global crawl_queue

    # TODO: url sampling
    sample_len = min(len(crawl_queue.queue), int(data))
    if sample_len:
        sampled_list = random.sample(crawl_queue.queue, sample_len)
        crawl_queue.queue = filter(lambda item: item not in sampled_list,
            crawl_queue.queue)
    else:
        sampled_list = []
    return make_response_ok(sampled_list)

def s_put(data, environ):
    global crawl_queue

    try:
        len(data)
    except TypeError:
        return make_response_failure("handling action PUT. data `%s` is not list" % data)
    crawl_queue.queue += data
    return make_response_ok(None)

def parse_query(query):
    args = {}
    pairs = query.split('&')
    for pair in pairs:
        if not '=' in pair:
            continue # FIXME: raise exception, return 400 to client
        key, value = pair.split('=', 1)
        args[key] = value
    return args

def dispatcher(request, environ):
    args = parse_query(request.query)
    raw_json = urllib.url2pathname(args['r'])
    message = protocol.ProtocolMessage(None, raw=raw_json)
    if message.action == 'GET':
        answer = s_get(message.data, environ)
        return json.encode(answer)
    elif message.action == 'PUT':
        answer = s_put(message.data, environ)
        return json.encode(answer)
    else:
        raise Exception, "Bad action %s" % message.action

def parse_request(environ):
    r = Request()
    r.method = environ['REQUEST_METHOD']
    r.path = environ['PATH_INFO']
    r.query = environ['QUERY_STRING']
    r.http_user_agent = environ['HTTP_USER_AGENT']
    r.http_host = environ['HTTP_HOST']
    r.server_protocol = environ['SERVER_PROTOCOL']
    r.remote_addr = environ['REMOTE_ADDR']
    return r

def handle_request(environ, start_response):
    request = parse_request(environ)
    content = dispatcher(request, environ)
    start_response('200 OK', [
        ('Content-Type', 'application/json'),
        ('Content-Length', str(len(content))),
        ])
    return content

def server_run():
    global crawl_queue

    crawl_queue = CrawlQueue(misc.params.queue_path)
    try:
        crawl_queue.load()
        server = WSGIServer((misc.params.address, misc.params.port), wsgiref.simple_server.WSGIRequestHandler)
        server.init_socket()
        server.set_app(handle_request)
        debug("Accepting connections...")
        while server.is_alive:
            server.handle_request()
    finally:
        debug("Gracefully shutting down")
        crawl_queue.save()

def main():
    usage_info = "Usage: %prog [OPTION...]"
    version_info = "heroshi queue server %s" % HEROSHI_VERSION
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False,
        address='0.0.0.0', port=protocol.BIND_PORT, queue_path=QUEUE_PATH)
    opt_parser.add_option('-q', '--quiet', action="store_true", help="Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true", help="Be verbose, print detailed information")
    opt_parser.add_option('-a', '--address', help="Queue manager IP address", metavar="IP_ADDRESS")
    opt_parser.add_option('-p', '--port', type="int", help="Queue manager IP port", metavar="PORT")
    opt_parser.add_option('-Q', '--queue-path', help="Queue location", metavar="FILE")
    # TODO: queue location
    opt_parser.add_option('-t', '--test', action="store_true", dest="run_tests", help="Run internal tests")
    (options, args) = opt_parser.parse_args()
    misc.params = options
    server_run()

if __name__ == '__main__':
    main()
