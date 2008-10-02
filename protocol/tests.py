# -*- coding: utf-8 -*-

import os
import unittest
import urllib2
import manager
import cPickle
import protocol
import misc
from twisted.internet.error import ConnectionDone, ConnectionLost


class DefaultParams(object):
    quiet = False


class QueueServerProtocolTestCase(unittest.TestCase):
    """Queue server side of protocol tests"""

    def setUp(self):
        misc.params = DefaultParams()
        self.queue_path = '/tmp/heroshi-test.queue'
        self.queue = manager.CrawlQueue(self.queue_path)

    def tearDown(self):
        pass

#     def test_get_001(self):
#         pass
#
#     def test_put_001(self):
#         pass

