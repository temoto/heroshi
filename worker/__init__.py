# -*- coding: utf-8 -*-

"""Crawler worker.
Gets URLs to crawl from queue server, crawls them, store and send crawl info back to queue server"""

import sys, os, time
from optparse import OptionParser
import cPickle
import random
from BeautifulSoup import BeautifulSoup
import twisted.web.client
from twisted.internet import reactor
import datetime

from protocol import BIND_PORT, ProtocolMessage
import shared.misc
from shared.misc import HEROSHI_VERSION, debug
from shared.link import Link
from shared.page import Page
from shared.storage import save_page


def random_useragent():
    USER_AGENTS = [
        'HeroshiBot/%s' % HEROSHI_VERSION,
        'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008072820 Firefox/3.0.1',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1) Gecko/20061010 Firefox/2.0',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows 98; Win 9x 4.90)',
    ]
    return random.choice(USER_AGENTS)

class Crawler(object):
    max_queue_size = 0
    max_connections = 0
    queue = []
    follow_depth = 0 # stay on root site
    pages = []
    page_root = '~/.heroshi/page'
    closed = False
    server_address = None
    server_port = 0
    active_connections = 0
    _queue_update_timer = None
    _queue_process_timer = None

    def __init__(self):
        self.server_address = shared.misc.params.address
        self.server_port = shared.misc.params.port
        self.max_queue_size = shared.misc.params.queue_size
        self.max_connections = shared.misc.params.connections
        debug("crawler started. Server: %s:%d, queue: %d, connections: %d" % (self.server_address, self.server_port, self.max_queue_size, self.max_connections))

    def status(self):
        return {
            'closed': self.closed,
            'server_address': self.server_address,
            'server_port': self.server_port,
            'pages_count': len(self.pages),
            'active_connections': self.active_connections,
            'max_connections': self.max_connections,
            'queue_length': len(self.queue),
            'max_queue_size': self.max_queue_size,
            'follow_depth': self.follow_depth,
        }

    def push_link(self, link):
        for page in self.pages:
            if page.link.full == link.full:
                return
        if self.active_connections < self.max_connections:
            debug("running %d of %d max connections" % (self.active_connections, self.max_connections))
        else:
            debug("but we have no free connections: all %d are busy" % self.max_connections)
            self.queue.append(link) # push link to queue for future processing
            return
        worker = twisted.web.client.getPage(str(link.full))
        worker.addCallback(self.on_worker_done, worker, link)
        worker.addErrback(self.on_worker_error, worker, link)
        self.active_connections += 1

    def crawl(self):
        self._queue_update_timer = reactor.callLater(5, self.on_queue_update_timer)
        self._queue_process_timer = reactor.callLater(10, self.on_queue_process_timer)
        reactor.run()

    def on_worker_done(self, page_content, worker, link):
        debug("page successfully crawled: %s (%d bytes)" % (link.full, len(page_content)))
        self.active_connections -= 1
        try:
            page = Page(link, page_content)
            page.find_links()
            page.visited = datetime.datetime.now()
            save_page(page, self.page_root)
            self.pages.append(page)
            for link in page.links:
                link.parent = page_link
                self.push_link(link)
        except ValueError, error:
            debug("worker value error: %s" % error)
        except Exception, error:
            debug("other error: %s" % error)
            sys.exit(1)

    def on_worker_error(self, worker, link):
        debug("page crawling error: %s" % link)
        self.active_connections -= 1

    def queue_get(self):
        num = self.max_queue_size - len(self.queue)
        if num < 1:
            debug("queue is full")
            return
        debug("getting %d items from %s:%d" % (num, self.server_address, self.server_port))
        message = ProtocolMessage('GET', data=num)
        job = twisted.web.client.getPage(
            message.get_http_request_url(self.server_address, self.server_port))
        job.addCallback(self.on_queue_get_done)
        job.addErrback(self.on_queue_request_error)

    def on_queue_get_done(self, content):
        debug("got raw data: %s" % content)
        message = ProtocolMessage('GET', raw=content)
        debug("decoded data: %s" % message.data)
        debug("updating queue with %d items" % len(message.data))
        for item_url, item_params in message.data.iteritems():
            for qi in self.queue:
                if qi.full == item_url:
                    break
            else:
                link = Link(item_url, parent_link=item_params.get('parent'))
                self.queue.append(link)

    def on_queue_request_error(self, error):
        debug("queue request error: %s" % error)

    def queue_put(self):
        def make_put_list():
            return [ {
                page.link.full: {
                    'visited': page.visited,
                    'parent': page.link.parent,
                }
            } for page in self.pages ]
        items = make_put_list()
        debug("putting %d %s into server" % (len(items), "item" if len(items) == 1 else "items"))
        message = ProtocolMessage('PUT', data=items)
        job = twisted.web.client.getPage(
            message.get_http_request_url(self.server_address, self.server_port))
        job.addCallback(self.on_queue_put_done)
        job.addErrback(self.on_queue_request_error)

    def on_queue_put_done(self, content):
        debug("got answer from queue server: %s" % content)
        # TODO: process the answer

    def on_queue_update_timer(self):
        debug("it's queue update time!")
        self._queue_update_timer = reactor.callLater(20, self.on_queue_update_timer)
        debug("checking if %d < 10%% of %d" % (len(self.queue), self.max_queue_size))
        if len(self.queue) < self.max_queue_size * 0.1:
            self.queue_get()
        if len(self.pages) > 0:#self.max_queue_size * 0.4:
            self.queue_put()

    def on_queue_process_timer(self):
        debug("queue process timer called")
        self._queue_process_timer = reactor.callLater(20, self.on_queue_process_timer)
        if len(self.queue):
            link = self.queue.pop()
            self.push_link(link)
        else:
            debug("nothing to crawl")


def put_and_exit(url):
    item = {url: {'visited': None, 'parent': None}}
    message = ProtocolMessage('PUT', data=[item])
    msg_url = message.get_http_request_url(shared.misc.params.address, shared.misc.params.port)
    worker = twisted.web.client.getPage(msg_url)
    worker.addCallback(lambda page: reactor.stop())
    worker.addErrback(lambda : reactor.stop())
    reactor.run()

def main():
    usage_info = "Usage: %prog [OPTION...] --address ADDRESS [--port PORT]"
    version_info = "heroshi worker %s" % HEROSHI_VERSION
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, queue_size=2000, connections=250, port=BIND_PORT)
    opt_parser.add_option('-q', '--quiet', action="store_true", help="Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true", help="Be verbose, print detailed information")
    opt_parser.add_option('-Q', '--queue-size', help="Maximum queue size. Crawler will ask queue server for this many of items. [Default = 2000]", metavar="N")
    opt_parser.add_option('-c', '--connections', help="Maximum number of open connections. This number cannot be larger than queue size. [Default = 250]", metavar="N")
    opt_parser.add_option('--put', help="Put URL in server queue", metavar="URL")
    opt_parser.add_option('-a', '--address', help="Queue manager IP address", metavar="IP_ADDRESS")
    opt_parser.add_option('-p', '--port', type="int", help="Queue manager IP port. [Default = %d]" % BIND_PORT, metavar="PORT")
    (options, args) = opt_parser.parse_args()
    shared.misc.params = options
    if not options.address:
        print("Address is required")
        opt_parser.print_help()
        sys.exit(2)
    if options.put:
        put_and_exit(options.put)
        sys.exit()
    crawler = Crawler()
    crawler.crawl()


if __name__ == '__main__':
    main()
