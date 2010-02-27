import os
import unittest
import urllib2
import cPickle

import shared.kot2
import shared.call_indicator
from worker import Crawler
from shared.link import Link
from shared.page import Page
from shared import BIND_PORT


class WorkerTestCase(unittest.TestCase):
    """Heroshi worker tests"""

    def setUp(self):
        self.client = Crawler(
                server=('localhost', BIND_PORT),
                queue_size=2000,
                max_connections=20,
                )
        shared.call_indicator.install_simple(shared.kot2, 'grab_multi')

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

    def test_push_link_001(self):
        """crawler.push_link() must call getPage()"""

        link = Link('http://localhost/')
        self.client.push_link(link)
        self.assertTrue(shared.call_indicator.is_called('twisted.web.client.getPage'))
