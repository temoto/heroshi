"""Crawler worker.

Gets URLs to crawl from queue server, crawls them, store and send crawl info back to queue server."""

import sys
import random
from datetime import datetime
import pyev as ev, signal
import pprint

from shared.conf import settings
from shared.error import ApiError
from shared.kot2 import grab_multi
from shared.page import Page
from shared.link import Link
from shared import TIME_FORMAT, REAL_USER_AGENT
from shared import api
from shared.misc import get_logger, log_exceptions
log = get_logger()


USER_AGENTS = [
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008072820 Firefox/3.0.1",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1) Gecko/20061010 Firefox/2.0",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows 98; Win 9x 4.90)",
]

def random_useragent():
    return random.choice(USER_AGENTS)


class Crawler(object):
    max_queue_size = 0
    max_connections = 0
    queue = []
    reports = []
    page_root = '~/.heroshi/page'
    configured = False
    closed = False
    server_address = None
    _update_timer = None
    _process_timer = None

    def __init__(self, server, queue_size, max_connections):
        self.server = server
        self.max_queue_size = queue_size
        self.max_connections = max_connections
        log.debug("crawler started. Server: %s, max queue size: %d, connections: %d",
                self.server_address, self.max_queue_size, self.max_connections)

    def status(self):
        return {
            'configured': self.configured,
            'closed': self.closed,
            'server_address': self.server_address,
            'report_count': len(self.reports),
            'max_connections': self.max_connections,
            'queue_length': len(self.queue),
            'max_queue_size': self.max_queue_size,
        }

    def crawl(self):
        self._loop = ev.default_loop()

        self._sigint_watcher = ev.Signal(signal.SIGINT, self._loop, self.on_sigint)
        self._sigint_watcher.start()

        self._update_timer = ev.Timer(settings.update_interval, settings.update_interval, self._loop, self.on_update)
        self._process_timer = ev.Timer(settings.process_interval, settings.process_interval, self._loop, self.on_process)

        self._configure_timer = ev.Timer(0, 0, self._loop, self.on_configure)
        self._configure_timer.start()

        self._loop.loop()

    def stop(self):
        self._loop.unloop()

    def queue_get(self):
        num = self.max_queue_size - len(self.queue)
        if num < 1:
            log.debug("  queue is full")
            return
        log.debug("  getting %d items from %s", num, self.server_address)
        try:
            new_queue = api.get_crawl_queue(self.max_queue_size)
            log.debug("  got %d items", len(new_queue))

            # extend worker queue
            # 1. update duplicate URLs
            for new_item in new_queue:
                for queue_item in self.queue:
                    if queue_item['url'] == new_item['url']: # compare URLs
                        self.queue.remove(queue_item)
            # 2. extend queue with new items
            self.queue.extend(new_queue)
        except ApiError:
            log.exception("")

    def queue_put(self):
        try:
            api.report_results(self.reports)
        except ApiError:
            log.exception("")
        finally:
            self.reports = []

    @log_exceptions
    def on_update(self, watcher, events):
        log.debug("it's queue update time!")
        msg_queue = "  checking queue size %d < 10%% of max %d..." % (len(self.queue), self.max_queue_size)
        if len(self.queue) < self.max_queue_size * 0.1:
            msg_queue += " Queue is not full enough. Requesting more items"
            self.queue_get()
        log.debug(msg_queue)
        msg_report = "  checking reports size %d > 0..." % (len(self.reports),)
        if len(self.reports) > 0:#self.max_queue_size * 0.4:
            msg_report += " Got enough reports. Sending"
            self.queue_put()
        log.debug(msg_report)

    @log_exceptions
    def on_process(self, watcher, events):
        log.debug("it's queue process time!")
        if len(self.queue):
            # pop a slice out of queue
            queue_slice = self.queue[:20] # XXX: magic number
            del self.queue[:20] # XXX: magic number
            log.debug("  crawling %d items: %s",
                    len(queue_slice), pprint.pformat(queue_slice))

            # convert queue slice into kot2.grab_multi URL list
            grab_now_list = [ (item['url'], item.get('parent')) for item in queue_slice ]
            results = grab_multi(grab_now_list)
            log.debug("  here are results: %s", pprint.pformat(results))

            def process_result(r_item):
                url, result = r_item
                timestamp = datetime.now().strftime(TIME_FORMAT)
                report = {'url': url,
                          'visited': timestamp,
                         }
                report.update(result.__dict__)
                if result.result:
                    page = Page(Link(url), result.content)
                    page.parse()
                    report['links'] = [ link.full for link in page.links ]
                return report

            reports = map(process_result, results.iteritems())

            self.reports.extend(reports)
        else:
            log.debug("  nothing to crawl")

    def set_configure_timer(self, after, repeat):
        self._configure_timer.stop()
        self._configure_timer.set(after, repeat)
        self._configure_timer.start()

    @log_exceptions
    def on_configure(self, watcher, events):
        log.debug("it's configure time!")
        try:
            new_config = api.configure()
            control = new_config.get('control')
            recheck_interval = new_config.get('recheck-interval')

            if control == api.CONTROL_SUSPEND:
                if recheck_interval is None:
                    log.critical("  got control: suspend without recheck-interval. Don't know what to do")
                    self.stop()
                else:
                    log.info("  configured to suspend for %s seconds", recheck_interval)
                    self._process_timer.stop()
                    self._update_timer.stop()
                    self.set_configure_timer(recheck_interval, 0)
            elif control == api.CONTROL_RESUME:
                if recheck_interval is None:
                    log.warning("  got control: resume without recheck-interval. Set recheck-interval to default")
                    recheck_interval = settings.default_recheck_interval
                log.info("  configured to resume. Next configure in %s seconds", recheck_interval)
                self._process_timer.start()
                self._update_timer.start()
                self.set_configure_timer(recheck_interval, 0)
            else:
                log.error("  unknown control value: %s", control)
                self.stop()
        except ApiError:
            log.exception("on_configure")
            self.set_configure_timer(30, 0)

    @log_exceptions
    def on_sigint(self, watcher, events):
        watcher.stop()
        watcher.loop.unloop()
