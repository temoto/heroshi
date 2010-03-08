import eventlet
import httplib
# pylint-silence unused imports required for mocking
import httplib2 # pylint: disable-msg=W0611
import socket
import smock
import unittest

from heroshi.conf import load_from_dict as conf_from_dict, settings
from heroshi.worker import Crawler
from heroshi import api # pylint: disable-msg=W0611

eventlet.monkey_patch(all=False, socket=True, select=True)


def mock_httplib2_request_404(_url, *_args, **_kwargs):
    resp = httplib.HTTPResponse(socket.socket())
    resp.status = 404
    return resp, ""


class WorkerTestCase(unittest.TestCase):
    """Heroshi worker tests."""

    def setUp(self):
        settings.manager_url = "fake-url"
        settings.socket_timeout = 10
        settings.identity = {'user_agent': "foo"}
        self.client = Crawler(
                queue_size=2000,
                max_connections=20,
                )

    def tearDown(self):
        smock.cleanup()
        conf_from_dict({})

    def test_crawl_001(self):
        """Must call `api.get_crawl_queue()`."""

        def mock_get_crawl_queue(_limit):
            self.client.stop()
            return []

        smock.mock('api.get_crawl_queue', returns_func=mock_get_crawl_queue)
        with eventlet.Timeout(1, False):
            self.client.crawl()
        self.assertTrue(self.client.closed)
        self.assertTrue(smock.is_called('api.get_crawl_queue'))

    def test_crawl_002(self):
        """Must call `httplib2.Http.request` and `report_item`."""

        def mock_get_crawl_queue(_limit):
            eventlet.sleep(0.05)
            return [{'url': "http://localhost/test_crawl_002_link", 'visited': None, 'links': []}]

        def mock_report_result(report):
            self.assertEqual(report['url'], "http://localhost/test_crawl_002_link")

        smock.mock('api.get_crawl_queue', returns_func=mock_get_crawl_queue)
        smock.mock('api.report_result', returns_func=mock_report_result)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request_404)
        with eventlet.Timeout(1, False):
            self.client.crawl()
        self.assertTrue(smock.is_called('httplib2.Http.request'))
        self.assertTrue(smock.is_called('api.report_result'))

    def test_crawl_003(self):
        """Must make no more than 5 simultaneous connections to single server."""

        item = {'url': "http://localhost/test_crawl_003_link", 'visited': None, 'links': []}
        flags = {'max_count': 0}
        def mock_httplib2_request_sleep(_url, *_args, **_kwargs):
            if 'http:localhost' in self.client._connections._pools:
                pool = self.client._connections._pools['http:localhost']
                flags['max_count'] = max(flags['max_count'], pool.max_size - pool.free())
            eventlet.sleep(0.05)
            raise socket.timeout()

        smock.mock('api.get_crawl_queue', returns=[])
        smock.mock('api.report_result', returns=None)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request_sleep)
        # prepopulate the queue
        for _ in xrange(10):
            self.client.queue.put(item)
        with eventlet.Timeout(1, False):
            self.client.crawl()
        self.assertTrue(0 < flags['max_count'] <= 5)

