"""Crawler worker.

Gets URLs to crawl from queue server, crawls them, store and send crawl info back to queue server."""

from datetime import datetime
import eventlet
from eventlet import GreenPool, greenthread, sleep, spawn
from eventlet.queue import Empty, Queue
import httplib2
import random, socket, sys

# from heroshi. ...
from data import PoolMap, Link, Page
from shared.conf import settings
from shared.error import ApiError
from shared import TIME_FORMAT, REAL_USER_AGENT
from shared import api
from shared.misc import get_logger
log = get_logger()

eventlet.monkey_patch(all=False, socket=True, select=True)


USER_AGENTS = [
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008072820 Firefox/3.0.1",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1) Gecko/20061010 Firefox/2.0",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows 98; Win 9x 4.90)",
]

def random_useragent():
    return random.choice(USER_AGENTS)


class Crawler(object):
    def __init__(self, queue_size, max_connections):
        self.max_queue_size = queue_size
        self.max_connections = max_connections

        self.queue = Queue(self.max_queue_size)
        self.closed = False
        self._handler_pool = GreenPool(self.max_connections)
        self._connections = PoolMap(httplib2.Http, pool_max_size=5, timeout=120)

        log.debug("Crawler started. Max queue size: %d, connections: %d.",
                  self.max_queue_size, self.max_connections)

    def crawl(self):
        # TODO: do something special about signals?

        crawler_thread = greenthread.getcurrent()
        def _exc_link(gt):
            try:
                gt.wait()
            except Exception:
                crawler_thread.throw(*sys.exc_info())

        def qputter():
            while True:
                if self.queue.qsize() < self.max_queue_size:
                    self.do_queue_get()
                    sleep()
                else:
                    sleep(settings.full_queue_pause)

        spawn(qputter).link(_exc_link)

        while not self.closed:
            sleep()
            # `get_nowait` will only work together with sleep(0) here
            # because we need switches to reraise exception from `do_process`.
            try:
                item = self.queue.get_nowait()
            except Empty:
                sleep(0.01)
                continue
            self._handler_pool.spawn(self.do_process, item).link(_exc_link)

    def stop(self):
        self.closed = True

    def do_queue_get(self):
        log.debug("It's queue update time!")
        num = self.max_queue_size - self.queue.qsize()
        log.debug("  getting %d items from URL server.", num)
        try:
            new_queue = api.get_crawl_queue(num)
            log.debug("  got %d items", len(new_queue))

            if len(new_queue) is 0:
                log.debug("  waiting some time before another request to URL server.")
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
            log.exception("do_queue_get")
            self.stop()

    def report_item(self, item):
        import cPickle
        pickled = cPickle.dumps(item)
        log.debug(u"Reporting %s results back to URL server. Size ~= %d KB. Connections cache: %r.",
                  unicode(item['url']),
                  len(pickled) / 1024,
                  self._connections)
        try:
            api.report_results( [item] )
        except ApiError:
            log.exception("report_item")

    def do_process(self, item):
        url = item['url']
        report = {'url': url}

        uri = httplib2.iri2uri(url)
        (scheme, authority, _path, _query, _fragment) = httplib2.parse_uri(uri)
        if scheme is None or authority is None:
            log.warning(u"Skipping invalid URI: %s", unicode(uri))
            return
        conn_key = scheme+":"+authority

        log.debug(u"Crawling: %s", url)
        conn = self._connections.get(conn_key, timeout=settings.socket_timeout)
        try:
            response, content = conn.request(url, headers={'user-agent': REAL_USER_AGENT})
        except KeyboardInterrupt:
            raise
        except socket.timeout:
            log.info(u"Socket timeout at %s", url)
            report['result'] = u"Socket timeout"
        except Exception, e:
            log.warning(u"HTTP error at %s: %s", url, str(e))
            report['result'] = u"HTTP Error: " + unicode(e)
        else:
            report['status_code'] = response.status
            report['content'] = content
            if response.status == 200:
                page = Page(Link(url), content)
                try:
                    page.parse()
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    report['result'] = u"Parse Error: " + unicode(e)
                else:
                    report['links'] = [ link.full for link in page.links ]
        finally:
            self._connections.put(conn_key, conn)

        timestamp = datetime.now().strftime(TIME_FORMAT)
        report['visited'] = timestamp
        self.report_item(report)
