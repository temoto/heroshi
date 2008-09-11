# -*- coding: utf-8 -*-

"""Crawler worker.
Gets URLs to crawl from queue server, crawls them, store and send crawl info back to queue server"""

import sys, os, time
from optparse import OptionParser
import cPickle
import random
from BeautifulSoup import BeautifulSoup
from twisted.web.client import getPage
from twisted.internet import reactor

from client_protocol import BIND_PORT, ActionGet, ActionPut, ActionClose, QueueClientFactory
import misc
from misc import HEROSHI_VERSION, debug
from link import Link
from page import Page
from storage import save_page


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
    _pool = []
    _queue_update_timer = None
    _queue_process_timer = None

    def __init__(self):
        self.server_address = misc.params.address
        self.server_port = misc.params.port
        self.max_queue_size = misc.params.queue_size
        self.max_connections = misc.params.connections
        debug("crawler started. Server: %s:%d, queue: %d, connections: %d" % (self.server_address, self.server_port, self.max_queue_size, self.max_connections))

    def is_link_crawled(self, link):
        for page in self.pages:
            if page.link.full == link.full:
                return True
        else:
            return False

    def process_page(self, page_link, page_content):
        try:
            page = Page(page_link, page_content)
            page.find_links()
            save_page(page, self.page_root)
            self.pages.append(page)
            for link in page.links:
                self.push_link(link)
        except ValueError, error:
            debug("worker value error: %s" % error)
        except Exception, error:
            debug("other error: %s" % error)
            sys.exit(1)

    def push_link(self, link):
        if self.is_link_crawled(link):
            return
        if len(self._pool) < self.max_connections:
            debug("running %d of %d max connections" % (len(self._pool), self.max_connections))
        else:
            debug("but we have no free connections: all %d are busy" % self.max_connections)
            self.queue.append(link) # push link back to queue
            return
        worker = getPage(str(link.full))
        worker.addCallback(self.on_worker_done, worker, link)
        worker.addErrback(self.on_worker_error, worker, link)
        self._pool.append(worker)

    def crawl(self):
        self._queue_update_timer = reactor.callLater(5, self.on_queue_update_timer)
        self._queue_process_timer = reactor.callLater(10, self.on_queue_process_timer)
        reactor.run()

    def on_worker_done(self, page, worker, link):
        debug("page successfully crawled: %s (%d bytes)" % (link.full, len(page)))
        self.process_page(link, page)
        self._pool.remove(worker)

    def on_worker_error(self, worker, link):
        debug("page crawling error: %s" % link)

    def on_queue_update(self, protocol, reason):
        debug("updating queue with %d items" % len(protocol.factory.items))
        for item in protocol.factory.items:
            for qi in self.queue:
                if qi.full == item:
                    break
            else:
                link = Link(item)
                self.queue.append(link)

    def queue_get(self):
        num = self.max_queue_size - len(self.queue)
        if num < 1:
            debug("queue is full")
            return
        debug("getting %d items from %s:%d" % (num, self.server_address, self.server_port))
        action = ActionGet(num)
        cf = QueueClientFactory([action])
        cf.on_disconnect = self.on_queue_update
        reactor.connectTCP(self.server_address, self.server_port, cf)

    def queue_put(self):
        items = [ page.link.full for page in self.pages ]
        debug("putting %d %s into server" % (len(items), "item" if len(items) == 1 else "items"))
        cf = QueueClientFactory([ActionPut(items=items)])
        reactor.connectTCP(self.server_address, self.server_port, cf)

    def on_queue_update_timer(self):
        debug("queue update timer called")
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


def put_and_exit(item):
    cf = QueueClientFactory([ActionPut(items=[item]), ActionClose()])
    reactor.connectTCP(misc.params.address, misc.params.port, cf)
    reactor.run()

def tests():
    test_server = '127.0.0.1'
    cf = QueueClientFactory([ActionGet(100), ActionClose()])
    reactor.connectTCP(test_server, misc.params.port, cf)
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
    opt_parser.add_option('-t', '--test', action="store_true", dest="run_tests", help="Run internal tests")
    (options, args) = opt_parser.parse_args()
    misc.params = options
    if options.run_tests:
        tests()
        sys.exit()
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

