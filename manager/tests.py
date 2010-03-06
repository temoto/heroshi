import os
import unittest
import urllib2
import cPickle

from . import manager


class ManagerTestCase(unittest.TestCase):
    """Heroshi queue server tests"""

    def setUp(self):
        shared.misc.params = DefaultParams()
        pass

    def tearDown(self):
        pass


class CrawlQueueTestCase(unittest.TestCase):
    def setUp(self):
        self.queue_path = '/tmp/heroshi-test.queue'

    def tearDown(self):
        pass

    def test_save_001(self):
        """CrawlQueue saving"""

        items = ['http://localhost/']
        cq = manager.CrawlQueue(self.queue_path)
        cq.queue[:] = items[:] # copy
        cq.save()
        f = open(self.queue_path, 'rb')
        read_items = cPickle.load(f)
        f.close()
        self.assertEqual(len(cq.queue), len(items))
        os.remove(self.queue_path)

    def test_load_001(self):
        """CrawlQueue loading"""

        items = ['http://localhost/']
        f = open(self.queue_path, 'wb')
        cPickle.dump(items, f)
        f.close()
        cq = manager.CrawlQueue(self.queue_path)
        cq.load()
        self.assertEqual(len(cq.queue), len(items))
        os.remove(self.queue_path)
