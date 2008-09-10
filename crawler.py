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

from client_protocol import BIND_PORT, ActionGet, ActionPut, QueueClientFactory
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

    def __init__(self, **kwargs):
        if 'server_address' in kwargs:
            self.server_address = kwargs['server_address']
        if 'server_port' in kwargs:
            self.server_port = kwargs['server_port']
        if 'queue_size' in kwargs:
            self.max_queue_size = kwargs['queue_size']
        if 'connections' in kwargs:
            self.max_connections = kwargs['connections']
        debug("crawler started. Server: %s:%d, queue: %d, connections: %d" % (self.server_address, self.server_port, self.max_queue_size, self.max_connections))

    def is_link_crawled(self, link):
        for page in self.pages:
            if page.link.full == link.full:
                return True
        else:
            return False

    def worker(self, link):
        debug("crawling %s" % link.full)
        try:
            if self.is_link_crawled(link):
                return
            urlfile = urllib2.urlopen(link.full)
            page_content = urlfile.read()
            page = Page(link, page_content)
            page.find_links()
            save_page(page, self.page_root)
            self.pages.append(page)
            for link in page.links:
                self.push_link(link)
        except ValueError, error:
            debug("worker value error: %s" % error)
        except urllib2.HTTPError, error:
            debug("worker HTTP error: %s" % error)
        except urllib2.URLError, error:
            debug("worker URL error: %s" % error)

    def error_handler(self, request, exc_info):
        exc_type, ex, tb = exc_info
        print(" ! error: %s" % ex)
        import traceback
        traceback.print_tb(tb)
        raise SystemExit

    def push_link(self, link):
        if self.is_link_crawled(link):
            debug("skipping already crawled link")
            return
        req = threadpool.WorkRequest(self.worker, args=[link], exc_callback=self.error_handler)
        self._pool.putRequest(req)

    def queue_get(self, num):
        debug("getting %d items from %s:%d" % (num, self.server_address, self.server_port))
        cf = QueueClientFactory([ActionGet(num)])
        reactor.connectTCP(self.server_address, self.server_port, cf)

    def queue_put(self, items):
        debug("putting %d %s into server" % (len(items), "item" if len(items) == 1 else "items"))
        cf = QueueClientFactory([ActionPut(items)])
        reactor.connectTCP(self.server_address, self.server_port, cf)

    def crawl(self):
        self.queue_get(self.max_queue_size)
        reactor.run()


def put_and_exit(item):
    crawler = Crawler(server_address=misc.params.address, server_port=misc.params.port)
    crawler.queue_put([item])
    reactor.run()

def tests():
    test_server = '127.0.0.1'
    crawler = Crawler(server_address=test_server, server_port=misc.params.port, queue_size=misc.params.queue_size, connections=misc.params.connections)
    crawler.crawl()

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

