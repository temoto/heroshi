import eventlet
# pylint-silence unused imports required for mocking
import httplib2 # pylint: disable-msg=W0611
import smock
import unittest

from heroshi.conf import load_from_dict as conf_from_dict, settings
from heroshi.worker import Crawler
from heroshi import api # pylint: disable-msg=W0611

eventlet.monkey_patch(all=False, socket=True, select=True)


# Very unprobably, but still may be you need to raise this value on slow machine if most tests fail.
DEFAULT_TIMEOUT = 0.02


def make_http_response(status_code):
    response = httplib2.Response({})
    response.status = status_code
    return response


class WorkerTestCase(unittest.TestCase):
    """Heroshi worker tests."""

    def setUp(self):
        settings.manager_url = "fake-url"
        settings.socket_timeout = 10
        settings.identity = {'name': "HeroshiBot", 'user_agent': "HeroshiBot/100.500 (lalala)"}
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
        with eventlet.Timeout(DEFAULT_TIMEOUT, False):
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
        smock.mock('httplib2.Http.request', returns=(make_http_response(404), ""))
        with eventlet.Timeout(DEFAULT_TIMEOUT, False):
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
            flags['max_count'] = max(flags['max_count'], self.client.get_active_connections_count('127.0.0.1'))
            eventlet.sleep(REQUEST_PAUSE)
            return make_http_response(404), ""

        smock.mock('api.get_crawl_queue', returns=[])
        smock.mock('api.report_result', returns=None)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request_sleep)
        # prepopulate the queue
        for _ in xrange(NUM_ITEMS):
            self.client.queue.put(item)
        with eventlet.Timeout(DEFAULT_TIMEOUT, False):
            self.client.crawl()
        self.assertTrue(self.client.queue.empty(), u"Crawler didn't consume all queue in allocated time.")
        self.assertTrue(self.client.graceful_stop(timeout=NUM_ITEMS * REQUEST_PAUSE),
                        u"Crawler didn't stop in allocated time.")
        self.assertTrue(flags['max_count'] > 0, u"No connections started at all.")
        self.assertTrue(flags['max_count'] <= self.client.max_connections_per_host,
                        u"Started too many connections.")


class RobotsTestCase(unittest.TestCase):
    """Heroshi worker robots.txt handling tests."""

    def setUp(self):
        settings.manager_url = "fake-url"
        settings.socket_timeout = 10
        settings.identity = {'name': "HeroshiBot", 'user_agent': "HeroshiBot/100.500 (lalala)"}
        self.client = Crawler(queue_size=2000, max_connections=20)

        self.uris = []
        self.responses = {}
        self.handlers = {}
        self.requested = []
        self.used_run_crawler = False
        self.on_unexpected_uri = 'fail'
        self.on_unexpected_uri_func = lambda url: self.fail(u"`self.on_unexpected_uri_func` is unset.")
        self.default_hanlder_200 = lambda url: (make_http_response(200), "Dummy page at %s." % (url,))
        self.default_hanlder_404 = lambda url: (make_http_response(404), "Not found: %s." % (url,))

        def mock_httplib2_request(url, *args, **kwargs):
            self.requested.append(url)
            if url in self.responses:
                code, content = self.responses[url]
                return make_http_response(code), content
            elif url in self.handlers:
                handler = self.handlers[url]
                return handler(url)
            else:
                if self.on_unexpected_uri == 'fail':
                    self.fail(u"Unknown URL requested: %s. You didn't register it in `self.uris`." % (url,))
                elif self.on_unexpected_uri == '200':
                    return self.default_hanlder_200(url)
                elif self.on_unexpected_uri == '404':
                    return self.default_hanlder_404(url)
                elif self.on_unexpected_uri == 'call':
                    return make_http_response(*self.on_unexpected_uri_func(url))
                else:
                    self.fail(u"Unknown URL requested: %s. And no code for `self.on_unexpected_uri`: %s." %
                              (url, self.on_unexpected_uri))
            self.fail(u"httplib2_request mock supposed to return somewhere earlier.")

        smock.mock('api.get_crawl_queue', returns=[])
        smock.mock('api.report_result', returns=None)
        smock.mock('httplib2.Http.request', returns_func=mock_httplib2_request)

    def tearDown(self):
        smock.cleanup()
        conf_from_dict({})

        assert self.used_run_crawler == True, \
                u"You supposed to run `self.run_crawler` in `RobotsTestCase` tests."

    def run_crawler(self):
        self.used_run_crawler = True

        assert sorted(self.uris) == sorted(set(self.uris)), \
                u"`RobotsTestCase` currently works only with a set of unique URIs. You provided some duplicates. "

        for uri in self.uris:
            item = {'url': uri, 'visited': None, 'links': []}
            self.handlers[uri] = self.default_hanlder_200
            self.client.queue.put(item)

        with eventlet.Timeout(DEFAULT_TIMEOUT, False):
            self.client.crawl()

    def test_self_001(self):
        """Quick self-test for testcase: fail on non-registered URI."""

        self.client.resolver.cache['dummy-valid.url'] = ['127.0.0.1']
        self.client.queue.put({'url': "http://dummy-valid.url/", 'visited': None, 'links': []})
        self.assertRaises(AssertionError, self.run_crawler)

    def test_request_robots_first(self):
        """For each new domain/port pair, must first request for /robots.txt."""

        self.uris.append("http://127.0.0.1/test_request_robots_first_link")
        self.on_unexpected_uri = '200'
        self.run_crawler()
        self.assertTrue(self.requested[0] == "http://127.0.0.1/robots.txt")

    def test_robots_401(self):
        """Must not crawl URL if /robots.txt yields 401."""

        URI = "http://127.0.0.1/test_robots_401_link"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 401, ""
        self.run_crawler()
        self.assertTrue(URI not in self.requested)

    def test_robots_403(self):
        """Must not crawl URL if /robots.txt yields 403."""

        URI = "http://127.0.0.1/test_robots_403_link"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 403, ""
        self.run_crawler()
        self.assertTrue(URI not in self.requested)

    def test_robots_404(self):
        """Must crawl URL if /robots.txt yields 404."""

        URI = "http://127.0.0.1/test_robots_404_link"
        self.uris.append(URI)
        self.handlers["http://127.0.0.1/robots.txt"] = self.default_hanlder_404
        self.run_crawler()
        self.assertTrue(URI in self.requested)

    def test_robots_empty(self):
        """Must crawl URL if /robots.txt is empty."""

        URI = "http://127.0.0.1/test_robots_empty_link"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 200, ""
        self.run_crawler()
        self.assertTrue(URI in self.requested)

    def test_robots_allows_all_to_star(self):
        """Must crawl URL if /robots.txt allows all robots everything."""

        URI = "http://127.0.0.1/test_robots_allows_all_to_star_link"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 200, "\
User-agent: *\r\n\
Disallow:"
        self.run_crawler()
        self.assertTrue(URI in self.requested)

    def test_robots_disallows_all_to_star(self):
        """Must not crawl URL if /robots.txt disallows all robots everything."""

        URI = "http://127.0.0.1/test_robots_allows_all_to_star_link"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 200, "\
User-agent: *\r\n\
Disallow: /"
        self.run_crawler()
        self.assertTrue(URI not in self.requested)

    def test_robots_disallows_all_to_heroshi(self):
        """Must not crawl URL if /robots.txt disallows HeroshiBot everything."""

        URI = "http://127.0.0.1/test_robots_disallows_all_to_heroshi"
        self.uris.append(URI)
        self.responses["http://127.0.0.1/robots.txt"] = 200, "\
User-agent: HeroshiBot\r\n\
Disallow: /"
        self.run_crawler()
        self.assertTrue(URI not in self.requested)

