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

        smock.mock('api.get_crawl_queue', returns=[])
        with eventlet.Timeout(0.05, False):
            self.client.crawl()
        self.assertTrue(smock.is_called('api.get_crawl_queue'))

    def test_crawl_002(self):
        """Must call `httplib2.Http.request` and `report_item`."""

        def mock_get_crawl_queue(_limit):
            return [{'url': "http://localhost/test_crawl_002_link", 'visited': None, 'links': []}]

        def mock_report_result(report):
            self.assertEqual(report['url'], "http://localhost/test_crawl_002_link")

        smock.mock('api.get_crawl_queue', returns_func=mock_get_crawl_queue)
        smock.mock('api.report_result', returns_func=mock_report_result)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request_404)
        with eventlet.Timeout(0.05, False):
            self.client.crawl()
        self.assertTrue(smock.is_called('httplib2.Http.request'))
        self.assertTrue(smock.is_called('api.report_result'))

    def test_crawl_003(self):
        """Must make no more than 5 simultaneous connections to single server."""

        item = {'url': "http://localhost/test_crawl_003_link", 'visited': None, 'links': []}
        flags = {'max_count': 0}
        NUM_ITEMS = self.client.max_connections_per_host * 2
        REQUEST_PAUSE = 0.05
        def mock_httplib2_request_sleep(url, *args, **kwargs): # pylint: disable-msg=W0613
            flags['max_count'] = max(flags['max_count'], self.client.get_active_connections_count('http:localhost'))
            eventlet.sleep(REQUEST_PAUSE)
            raise socket.timeout()

        smock.mock('api.get_crawl_queue', returns=[])
        smock.mock('api.report_result', returns=None)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request_sleep)
        # prepopulate the queue
        for _ in xrange(NUM_ITEMS):
            self.client.queue.put(item)
        with eventlet.Timeout(0.05, False):
            self.client.crawl()
        self.assertTrue(self.client.queue.empty(), u"Crawler didn't consume all queue in allocated time.")
        self.assertTrue(self.client.graceful_stop(timeout=NUM_ITEMS * REQUEST_PAUSE),
                        u"Crawler didn't stop in allocated time.")
        self.assertTrue(flags['max_count'] > 0, u"No connections started at all.")
        self.assertTrue(flags['max_count'] <= self.client.max_connections_per_host,
                        u"Started too many connections.")

