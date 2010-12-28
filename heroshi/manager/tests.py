from datetime import datetime
try:
    import yajl as json
except ImportError:
    import json
import smock
import unittest
import webob

from heroshi import TIME_FORMAT
from heroshi.conf import load_from_dict as conf_from_dict, settings
from heroshi.manager import Manager
# pylint-silence unused imports required for mocking
from heroshi.storage.postgres import StorageConnection # pylint: disable-msg=W0611


class ManagerTestCase(unittest.TestCase):
    """Heroshi URL server tests."""

    def setUp(self):
        settings.prefetch = {'queue_size': 10,
                             'get_timeout': 0.01,
                             'single_limit': 5,
                             'cache_timeout': 60,
                            }
        settings.postreport = {'queue_size': 10,
                               'flush_size': 1,
                               'flush_delay': 0.01,
                              }
        settings.storage = {'max_connections': 1}
        settings.api = {'max_queue_limit': 100}
        self.manager = Manager()
        # StorageConnection mock
        smock.mock('StorageConnection.__init__', returns=None)
        #self.manager.storage_connections.create = storage.StorageConnection

    def tearDown(self):
        self.manager.close()
        smock.cleanup()
        conf_from_dict({})

    def test_get_001(self):
        """Must call `storage.meta.query_new_random`."""

        req = webob.Request.blank('/')
        req.method = 'POST'
        req.body = "limit=10"

        smock.mock('StorageConnection.query_new_random', returns=[])
        self.manager.active = True
        self.manager.crawl_queue(req)
        self.assertTrue(smock.is_called('StorageConnection.query_new_random'))

    def test_get_002(self):
        """Must return list of items fetched from storage."""

        req = webob.Request.blank('/')
        req.method = 'POST'
        req.body = "limit=10"

        items = [{'url': "http://url1/", 'visited': None},
                 {'url': "http://url2/", 'visited': None},
                 {'url': "http://url3/", 'visited': None}]
        items_copy = items[:]
        smock.mock('StorageConnection.query_new_random',
                   returns_func=lambda *a, **kw: [items_copy.pop()] if items_copy else [])
        self.manager.active = True
        result = self.manager.crawl_queue(req)
        self.assertEqual(sorted(items), sorted(result))

    def test_get_003(self):
        """Must return no more than requested items."""

        req = webob.Request.blank('/')
        req.method = 'POST'
        req.body = "limit=2"

        items = [{'url': "http://url1/", 'visited': None},
                 {'url': "http://url2/", 'visited': None},
                 {'url': "http://url3/", 'visited': None}]
        items_copy = items[:]
        smock.mock('StorageConnection.query_new_random',
                   returns_func=lambda *a, **kw: [items_copy.pop()] if items_copy else [])
        result = self.manager.crawl_queue(req)
        self.assertTrue(len(result) <= 2)

    def test_put_001(self):
        """Must accept one valid report item."""

        req = webob.Request.blank('/')
        req.method = 'PUT'
        url = "http://localhost/manager-test_put_001-url"
        item = {'url': url, 'visited': datetime.now().strftime(TIME_FORMAT),
                'status_code': 200, 'content': "test content",
               }
        req.body = json.dumps(item)

        smock.mock('StorageConnection.save_content', returns=None)
        smock.mock('StorageConnection.query_all_by_url_one', returns={'url': url, 'visited': None})
        smock.mock('StorageConnection.save', returns=None)
        smock.mock('StorageConnection.update', returns=None)
        self.manager.report_result(req)
        # assert nothing is raised

