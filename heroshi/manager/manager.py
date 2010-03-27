"""Heroshi URL server implementation main module."""

__all__ = ['Manager']

import cjson
import datetime
import dateutil.parser
import eventlet, eventlet.pools, eventlet.queue
from eventlet import spawn, sleep, Queue
eventlet.monkey_patch(all=False, socket=True, select=True)

from heroshi import get_logger, log_exceptions
from heroshi.data import Cache
from heroshi.conf import settings
log = get_logger("manager")
from heroshi.profile import Profile
from heroshi.storage import StorageConnection


class Manager(object):
    """Class encapsulating Heroshi URL server state."""

    def __init__(self):
        self.active = False
        self.prefetch_queue = Queue(settings.prefetch['queue_size'])
        self.prefetch_thread = spawn(self.prefetch_worker)
        self.given_items = Cache()
        self.postreport_queue = Queue(settings.postreport['queue_size'])
        self.postreport_thread = spawn(self.postreport_worker)
        self.storage_connections = eventlet.pools.Pool(max_size=settings.storage['max_connections'])
        self.storage_connections.create = StorageConnection

    def close(self):
        self.active = False
        self.prefetch_thread.kill()
        self.postreport_thread.kill()

    def get_from_prefetch_queue(self, size):
        result = []
        while len(result) < size:
            sleep()
            try:
                pack = self.prefetch_queue.get(timeout=settings.prefetch['get_timeout'])
            except eventlet.queue.Empty:
                break
            result.extend(pack)
        return result

    def prefetch_worker(self):
        if not self.active:
            sleep(0.01)
        while self.active:
            with self.storage_connections.item() as storage:
                docs = storage.query_new_random(settings.prefetch['single_limit'])
            if len(docs) == 0:
                sleep(10.)
                continue
            else:
                # Note: putting a *list* as a single item into queue
                self.prefetch_queue.put(docs)
        # and respawn again
        self.prefetch_thread = spawn(self.prefetch_worker)

    @log_exceptions
    def _postreport_worker(self):
        docs = []
        while len(docs) < settings.postreport['flush_size']: # inner accumulator loop
            try:
                item = self.postreport_queue.get(timeout=settings.postreport['flush_delay'])
            except eventlet.queue.Empty:
                break

            # Quick dirty duplicate filtering.
            #     Note that this code only finds dups in current "flush pack". `report_result` uses
            # `is_duplicate_report` which finds dups in whole `postreport_queue` but it can't find dups here.
            # Thus two dups searchers.
            #     It is still possible that at most 2 duplicate reports exist: one in `postreport_queue`
            # and one in current "flush pack". This is acceptable, because most of the dups are filtered out.
            for doc in docs:
                if item['url'] == doc['url']:
                    item = None
                    break
            if item is None:
                continue

            if 'result' not in item:
                # It's a link, found on some reported page.
                # Just add it to bulk insert, don't try to update any document here.
                docs.append(item)
                continue

            try:
                old_doc = self.given_items[item['url']]
            except KeyError:
                log.warning(u"Item is not found in cache, doing lookup for %s.", item['url'])
                with self.storage_connections.item() as storage:
                    old_doc = storage.query_all_by_url_one(item['url']) or {}

            item = dict(old_doc, **item)
            docs.append(item)

        if not docs:
            return

        with self.storage_connections.item() as storage:
            for doc in docs[:]:
                content = doc.pop('content', None)

                if content is None:
                    continue
                content_type = doc.get('headers', {}).get('content-type', "application/octet-stream")

                if doc.get('_id') is None:
                    # this is a report on yet unknown URL
                    storage.save(doc, raise_conflict=True, force_update=True, batch=None)
                    docs.remove(doc)

                storage.save_content(doc, content, content_type, raise_conflict=False)

            storage.update(docs, raise_conflict=True, all_or_nothing=True, ensure_commit=True)

    def postreport_worker(self):
        if not self.active:
            sleep(0.01)

        while self.active:
            self._postreport_worker()

        # and respawn again
        self.prefetch_thread = spawn(self.prefetch_worker)

    @log_exceptions
    def crawl_queue(self, request):
        limit = max(int(request.POST['limit']), settings.api['max_queue_limit'])

        time_now = datetime.datetime.now()

        doc_list = self.get_from_prefetch_queue(limit)
        for doc in doc_list:
            self.given_items.set(doc['url'], doc, settings.prefetch['cache_timeout'])

        def is_old(doc):
            visited_str = doc['visited']
            if not visited_str:
                return True
            visited = dateutil.parser.parse(visited_str)
            diff = time_now - visited
            return diff > datetime.timedelta(minutes=settings.api['min_revisit_minutes'])

        doc_list = filter(is_old, doc_list)

        def make_queue_item(doc):
            filter_fields = ('url', 'headers', 'visited',)
            return dict( (k,v) for (k,v) in doc.iteritems() if k in filter_fields )

        queue = map(make_queue_item, doc_list)
        return queue

    def is_duplicate_report(self, url):
        """Quick dirty duplicate searching."""

        for doc in self.postreport_queue.queue:
            if url == doc['url']:
                return True
        return False

    def force_append_links(self, links):
        # 1. remove duplicates
        links = set(links)

        # 2. put links into queue
        for url in links:
            new_doc = {'_id': url, 'url': url, 'parent': None, 'visited': None}
            self.postreport_queue.put(new_doc)

    @log_exceptions
    def report_result(self, request):
        report = cjson.decode(request.body)

        # `report['links']` now used only to force insertion of new URLs into
        #   Heroshi crawling queue via bin/heroshi-append script.
        # So, if a more sophisticated way to append new URLs is to arise,
        #   remove this code.
        if report['url'] is None:
            self.force_append_links(report['links'])
            return

        if self.is_duplicate_report(report['url']):
            return

        # accept report into postreport_queue for later persistent saving
        try:
            doc = self.given_items[report['url']]
        except KeyError:
            self.postreport_queue.put(report)
        else:
            doc.update(report)
            self.postreport_queue.put(doc)

        return None
