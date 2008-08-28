# -*- coding: utf-8 -*-

import sys, os, time
import threading, threadpool
import urllib2
from BeautifulSoup import BeautifulSoup

from link import Link
from storage import save_page

HEROSHI_VERSION = '0.1'


class Page(object):
    link = None
    html_content = ''
    lang = ''
    text_content = u''
    links = []

    def __init__(self, link, content):
        self.link = link
        self.html_content = content

    def parse(self):
        soup = BeautifulSoup(self.html_content)
#         self.text_content = ''.join( [ e for e in soup.recursiveChildGenerator() if isinstance(e, unicode) ] )
#         print(" * debug parsed %d bytes of HTML into %d bytes of raw text" % (len(self.html_content), len(self.text_content)))
        a_tags = soup.findAll('a')
        for a in a_tags:
            href = a.get('href')
            if href:
                link = Link(href, self.link)
                self.links.append(link)
        print(" * debug extracted %d links" % len(self.links))

    def __unicode__(self):
        return "page at %s" % self.link.full


class Site(object):
    pass


class Crawler(object):
    _pool = None
    start_url = '' # start URL
    follow_depth = 0 # stay on root site
    threads = 1
    pages = []
    user_agent = 'HeroshiBot/%s' % HEROSHI_VERSION
    page_root = '~/.heroshi/page'

    def __init__(self, **kwargs):
        self.start_url = kwargs['start']
        if 'threads' in kwargs:
            self.threads = kwargs['threads']
        self._pool = threadpool.ThreadPool(self.threads)

    def is_link_crawled(self, link):
        for page in self.pages:
            if page.link.full == link.full: # TODO: check better
                return True
        else:
            return False

    def worker(self, link):
        print(" * debug crawling %s" % link.full)
        try:
            if self.is_link_crawled(link):
                return
            urlfile = urllib2.urlopen(link.full)
            page_content = urlfile.read()
            page = Page(link, page_content)
            page.parse()
            save_page(page, self.page_root)
            self.pages.append(page)
            for link in page.links:
                self.push_link(link)
        except ValueError, error:
            print(" * debug worker value error: %s" % error)
        except urllib2.HTTPError, error:
            print(" * debug worker HTTP error: %s" % error)
        except urllib2.URLError, error:
            print(" * debug worker URL error: %s" % error)

    def error_handler(self, request, exc_info):
        exc_type, ex, tb = exc_info
        print(" ! error: %s" % ex)
        import traceback
        traceback.print_tb(tb)
        raise SystemExit

    def push_link(self, link):
        if self.is_link_crawled(link):
            print(" * debug skipping already crawled link")
            return
        req = threadpool.WorkRequest(self.worker, args=[link], exc_callback=self.error_handler)
        self._pool.putRequest(req)

    def crawl(self):
        start_link = Link(self.start_url)
        self.push_link(start_link)
        self._pool.wait()


def main():
    if len(sys.argv) < 2:
        print("Usage: %s URL" % os.path.basename(sys.argv[0]))
        exit()
    # TODO: getopt parameters: start url, follow depth, threads count
    crawler = Crawler(start=sys.argv[1])
    try:
        crawler.crawl()
        time.sleep(1)
    except KeyboardInterrupt:
        print(" * debug ^C catched")

if __name__ == '__main__':
    main()


