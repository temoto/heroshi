# -*- coding: utf-8 -*-

import os
import unittest
import urllib2
import cPickle
from twisted.internet.error import ConnectionDone, ConnectionLost
import twisted.internet
import twisted.web.client

import misc, worker, protocol, manager, shared.call_indicator
from link import Link
from page import Page


class TestParams(object):
    quiet = False
    address = 'localhost'
    port = protocol.BIND_PORT
    queue_size = 2000
    connections = 20


class TestGetPageDeferred(object):
    def __init__(self, url):
        self.url = url

    def addCallback(self, *args, **kwargs):
        pass

    def addErrback(self, *args, **kwargs):
        pass


class WorkerTestCase(unittest.TestCase):
    """Heroshi worker tests"""

    def setUp(self):
        misc.params = TestParams()
        self.client = worker.Crawler()
        shared.call_indicator.install_simple(twisted.internet.reactor, 'run')
        shared.call_indicator.install_simple(twisted.internet.reactor, 'connectTCP')
        shared.call_indicator.install(twisted.web.client, 'getPage',
            lambda url: TestGetPageDeferred(url))

    def tearDown(self):
        shared.call_indicator.restore_all()
        shared.call_indicator.clean_all()

    def test_crawl_001(self):
        """crawler.crawl() must call reactor.run()"""

        self.client.crawl()
        self.assertTrue(shared.call_indicator.is_called('twisted.internet.reactor.run'))
        shared.call_indicator.clean_calls('run')

    def test_queue_get_001(self):
        """crawler.queue_get() must call getPage()"""

        self.client.queue_get()
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))
        shared.call_indicator.clean_calls('connectTCP')

    def test_queue_put_001(self):
        """crawler.queue_put() must call getPage()"""

        self.client.queue_put()
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))
        shared.call_indicator.clean_calls('connectTCP')

    def test_queue_put_002(self):
        """crawler.queue_put() with some links must call getPage()"""

        parent_link = Link('http://localhost/')
        links = [ Link('http://localhost/cat/%d/' % x, parent_link) for x in xrange(7) ]
        pages = [ Page(link, 'empty-page') for link in links ]
        self.client.pages += pages
        self.client.queue_put()
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))

    def test_queue_put_and_exit_001(self):
        """put_and_exit() must call getPage"""

        worker.put_and_exit('http://localhost')
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))
        self.assertTrue(shared.call_indicator.is_called('twisted.internet.reactor.run'))

    def test_push_link_001(self):
        """crawler.push_link() must call getPage()"""

        link = Link('http://localhost/')
        self.client.push_link(link)
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))


if __name__ == '__main__':
    unittest.main()
