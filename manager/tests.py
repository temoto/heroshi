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


class ManagerTestCase(unittest.TestCase):
    """Heroshi queue server tests"""

    def setUp(self):
        misc.params = DefaultParams()
        pass

    def tearDown(self):
        pass

#     def test_connect_001(self):
#         urllib2.urlopen
#         pass


class CrawlQueueTestCase(unittest.TestCase):
    def setUp(self):
        misc.params = DefaultParams()
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


if __name__ == '__main__':
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(CrawlQueueTestCase))
    suite.addTest(loader.loadTestsFromTestCase(ManagerTestCase))
    unittest.TextTestRunner(verbosity=2).run(suite)
