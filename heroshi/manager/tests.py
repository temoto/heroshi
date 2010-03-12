import cjson
from datetime import datetime
import smock
import unittest
import webob

# pylint-silence unused imports required for mocking
from heroshi import storage # pylint: disable-msg=W0611
from heroshi import TIME_FORMAT
from heroshi.conf import load_from_dict as conf_from_dict, settings
from heroshi.manager import manager


class ManagerTestCase(unittest.TestCase):
    """Heroshi URL server tests."""

    def setUp(self):
        settings.storage_root = "/tmp/heroshi-manager-test"

    def tearDown(self):
        smock.cleanup()
        conf_from_dict({})
        with manager.prefetch_worker_pool.item() as pre_w:
            pre_w.kill()
        with manager.postreport_worker_pool.item() as post_w:
            post_w.kill()

    def test_get_001(self):
        """Must call `storage.query_meta_new_random`."""

        req = webob.Request.blank('/')
        req.method = 'POST'
        req.body = "limit=10"

        smock.mock('storage.query_meta_new_random', returns=[])
        manager.crawl_queue(req)
        self.assertTrue(smock.is_called('storage.query_meta_new_random', manager.PREFETCH_SINGLE_LIMIT))

    def test_get_002(self):
        """Must return list of items fetched from storage."""

        req = webob.Request.blank('/')
        req.method = 'POST'
        req.body = "limit=10"

        items = [{'url': "http://url1/", 'visited': None},
                 {'url': "http://url2/", 'visited': None},
                 {'url': "http://url3/", 'visited': None}]
        items_copy = items[:]
        smock.mock('storage.query_meta_new_random',
                   returns_func=lambda *a, **kw: [items_copy.pop()] if items_copy else [])
        result = manager.crawl_queue(req)
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
        smock.mock('storage.query_meta_new_random',
                   returns_func=lambda *a, **kw: [items_copy.pop()] if items_copy else [])
        result = manager.crawl_queue(req)
        self.assertTrue(len(result) <= 2)

    def test_put_001(self):
        """Must accept one valid report item."""

        req = webob.Request.blank('/')
        req.method = 'PUT'
        url = "http://localhost/manager-test_put_001-url"
        item = {'url': url, 'visited': datetime.now().strftime(TIME_FORMAT),
                'status_code': 200, 'content': "test content",
               }
        req.body = cjson.encode(item)

        smock.mock('storage.save_content', returns=None)
        smock.mock('storage.query_meta_by_url_one', returns={'url': url, 'visited': None})
        smock.mock('storage.save_meta', returns=None)
        smock.mock('storage.update_meta', returns=None)
        manager.report_result(req)
        # assert nothing is raised

