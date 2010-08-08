"""Heroshi worker implementation.

Gets URLs to crawl from queue server, crawls them via io-worker,
sends crawl info back to queue server."""

from datetime import datetime
import errno
import eventlet
from eventlet import GreenPool, greenthread, sleep, spawn, with_timeout
from eventlet.queue import Empty, Queue
from eventlet.semaphore import Semaphore
import httplib, httplib2
import json
import random, time, urllib, urlparse
import robotparser
import subprocess

from heroshi import TIME_FORMAT
from heroshi import api, error, get_logger
log = get_logger("worker.Crawler")
from heroshi.conf import settings
from heroshi.data import Cache, PoolMap
from heroshi.error import ApiError, CrawlError, FetchError, RobotsError
from heroshi.misc import reraise_errors

eventlet.monkey_patch(all=False, os=True, socket=True, select=True)


class Crawler(object):
    def __init__(self, queue_size, max_connections):
        self.max_queue_size = queue_size
        self.max_connections = max_connections
        self.max_connections_per_host = 5

        self.queue = Queue(self.max_queue_size)
        self.closed = False
        self._handler_pool = GreenPool(self.max_connections)
        self._connections = PoolMap(object,
                                    pool_max_size=self.max_connections_per_host,
                                    timeout=120)
        self._robots_cache = PoolMap(self.get_robots_checker,
                                     pool_max_size=1,
                                     timeout=600)
        self.start_io()

        log.debug(u"Crawler started. Max queue size: %d, connections: %d.",
                  self.max_queue_size, self.max_connections)

    def crawl(self, forever=True):
        # TODO: do something special about signals?

        if forever:
            self.start_queue_updater()

        while not self.closed:
            # `get_nowait` will only work together with sleep(0) here
            # because we need greenlet switch to reraise exception from `do_process`.
            sleep()
            try:
                item = self.queue.get_nowait()
            except Empty:
                if not forever:
                    self.graceful_stop()
                sleep(0.01)
                continue
            t = self._handler_pool.spawn(self.do_process, item)
            t.link(reraise_errors, greenthread.getcurrent())

    def stop(self):
        self.closed = True
        self._io_worker.kill()

    def graceful_stop(self, timeout=None):
        """Stops crawler and waits for all already started crawling requests to finish.

        If `timeout` is supplied, it waits for at most `timeout` time to finish
            and returns True if allocated time was enough.
            Returns False if `timeout` was not enough.
        """
        self.closed = True
        self._io_worker.stdin.close()
        if timeout is not None:
            with eventlet.Timeout(timeout, False):
                if hasattr(self, '_queue_updater_thread'):
                    self._queue_updater_thread.kill()
                self._handler_pool.waitall()
                return True
            return False
        else:
            if hasattr(self, '_queue_updater_thread'):
                self._queue_updater_thread.kill()
            self._handler_pool.waitall()

    def get_active_connections_count(self, key):
        pool = self._connections._pools.get(key)
        if pool is None:
            return 0
        return pool.max_size - pool.free()

    def start_queue_updater(self):
        self._queue_updater_thread = spawn(self.queue_updater)
        self._queue_updater_thread.link(reraise_errors, greenthread.getcurrent())

    def queue_updater(self):
        while not self.closed:
            if self.queue.qsize() < self.max_queue_size:
                self.do_queue_get()
                sleep()
            else:
                sleep(settings.full_queue_pause)

    def do_queue_get(self):
        log.debug(u"It's queue update time!")
        num = self.max_queue_size - self.queue.qsize()
        log.debug(u"  getting %d items from URL server.", num)
        try:
            new_queue = api.get_crawl_queue(num)
            log.debug(u"  got %d items", len(new_queue))

            if len(new_queue) == 0:
                log.debug(u"  waiting some time before another request to URL server.")
                sleep(10.0)

            # extend worker queue
            # 1. skip duplicate URLs
            for new_item in new_queue:
                for queue_item in self.queue.queue:
                    if queue_item['url'] == new_item['url']: # compare URLs
                        break
                else:
                    # 2. extend queue with new items
                    self.queue.put(new_item)

            # shuffle the queue so there are no long sequences of URIs on same domain
            random.shuffle(self.queue.queue)
        except ApiError:
            log.exception(u"do_queue_get")
            self.stop()

    def io_reader(self):
        while not self.closed:
            result_str = self._io_worker.stdout.readline()
            if not result_str:
                sleep(0.050)
                continue
            decoded = json.loads(result_str)
            for k in decoded:
                decoded[k.lower()] = decoded.pop(k)
            status = decoded.pop('status')
            decoded.pop('success')
            decoded['result'] = u"OK" if status == u"200 OK" else u"non-200: " + status
            decoded['status_code'] = decoded.pop('statuscode')
            decoded['content'] = decoded.pop('body')
            self._io_results[decoded['url']] = decoded

    def start_io(self):
        self._io_worker = subprocess.Popen("io-worker/io-worker", bufsize=1,
                                           stdin=subprocess.PIPE, stdout=subprocess.PIPE)

        self._io_results = {}
        self._io_reader_thread = spawn(self.io_reader)
        self._io_reader_thread.link(reraise_errors, greenthread.getcurrent())
        self._io_write_sem = Semaphore()

    def report_item(self, item):
        import cPickle
        pickled = cPickle.dumps(item)
        log.debug(u"Reporting %s results back to URL server. Size ~= %d KB.",
                  unicode(item['url']),
                  len(pickled) / 1024)
        try:
            api.report_result(item)
        except ApiError:
            log.exception(u"report_item")

    def fetch(self, url):
        try:
            with self._io_write_sem:
                self._io_worker.stdin.write(url+'\n')
        except IOError, e:
            if e.errno == errno.EPIPE:
                log.info("IO worker is dead. Restarting...")
                self.start_io()
                sleep(1)
            else:
                raise

        while not self.closed:
            v = self._io_results.pop(url, None)
            if v is not None:
                return v
            sleep(0.100)

    def get_robots_checker(self, scheme, authority):
        """PoolMap func :: scheme, authority -> (agent, uri -> bool)."""
        robots_uri = "%s://%s/robots.txt" % (scheme, authority)

        fetch_result = self.fetch(robots_uri)
        if fetch_result['result'] == u"OK":
            # TODO: set expiration time from headers
            # but this must be done after `self._robots_cache.put` or somehow else...
            if 200 <= fetch_result['status_code'] < 300:
                parser = robotparser.RobotFileParser()
                parser.parse(fetch_result['content'].splitlines())
                return parser.can_fetch
            # Authorization required and Forbidden are considered Disallow all.
            elif fetch_result['status_code'] in (401, 403):
                return lambda _agent, _uri: False
            # /robots.txt Not Found is considered Allow all.
            elif fetch_result['status_code'] == 404:
                return lambda _agent, _uri: True
            # FIXME: this is an optimistic rule and probably should be detailed with more specific checks
            elif fetch_result['status_code'] >= 400:
                return lambda _agent, _uri: True
            # What other cases left? 100 and redirects. Consider it Disallow all.
            else:
                return lambda _agent, _uri: False
        else:
            raise FetchError(u"/robots.txt fetch problem: %s" % (fetch_result['result']))

    def ask_robots(self, uri, scheme, authority):
        key = scheme+":"+authority
        with self._robots_cache.getc(key, scheme, authority) as checker:
            try:
                return checker(settings.identity['name'], uri)
            except Exception, e:
                log.exception(u"Get rid of this. ask_robots @ %s", uri)
                raise RobotsError(u"Error checking robots.txt permissions for URI '%s': %s" % (uri, unicode(e)))

    def do_process(self, item):
        report = self._process(item)
        timestamp = datetime.now().strftime(TIME_FORMAT)
        report['visited'] = timestamp
        self.report_item(report)
        self.queue.task_done()

    def _process(self, item):
        url = item['url']
        log.debug(u"Crawling: %s", url)
        uri = httplib2.iri2uri(url)
        report = {
            'url': url,
            'result': None,
            'status_code': None,
            'visited': None,
        }

        (scheme, authority, _path, _query, _fragment) = httplib2.parse_uri(uri)
        if scheme is None or authority is None:
            report['result'] = u"Invalid URI"
        else:
            try:
                # this line is copied from robotsparser.py:can_fetch
                urllib.quote(urlparse.urlparse(urllib.unquote(url))[2])
            except KeyError:
                report['result'] = u"Malformed URL quoting."
                return report

            try:
                robot_check_result = self.ask_robots(uri, scheme, authority)
            except CrawlError, e:
                report['result'] = unicode(e)
                return report
            if robot_check_result == True:
                pass
            elif robot_check_result == False:
                report['result'] = u"Deny by robots.txt"
                return report
            else:
                assert False, u"This branch should not be executed."
                report['result'] = u"FIXME: unhandled branch in _process."
                return report

            fetch_start_time = time.time()

            fetch_result = with_timeout(settings.socket_timeout, self.fetch, uri, timeout_value='timeout')
            if fetch_result == 'timeout':
                fetch_result = {}
                report['result'] = u"Fetch timeout"
            fetch_result.pop('cached', None)

            fetch_end_time = time.time()
            report['fetch_time'] = int((fetch_end_time - fetch_start_time) * 1000)
            report.update(fetch_result)

        return report
