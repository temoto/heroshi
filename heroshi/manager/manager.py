"""Heroshi URL server implementation main module."""

__all__ = ['crawl_queue', 'report_result']

import cjson
import datetime
import dateutil.parser
import eventlet, eventlet.pools, eventlet.queue
eventlet.monkey_patch(all=False, socket=True, select=True)

from heroshi import storage
from heroshi.data import Cache
from heroshi.conf import settings
from heroshi.misc import get_logger, log_exceptions
log = get_logger("manager")


MAX_LIMIT = 1000
MIN_VISIT_TIMEDELTA = datetime.timedelta(hours=6)
PREFETCH_SINGLE_LIMIT = 100
PREFETCH_QUEUE_SIZE = 100
PREFETCH_GET_TIMEOUT = 0.2
POSTREPORT_QUEUE_SIZE = 10*1000
POSTREPORT_FLUSH_SIZE = 1000
POSTREPORT_FLUSH_DELAY = 10
STORAGE_MAX_CONNECTIONS = 4

prefetch_queue = eventlet.Queue(PREFETCH_QUEUE_SIZE)
prefetch_worker_pool = eventlet.pools.Pool(max_size=1)
prefetch_worker_pool.create = lambda: eventlet.spawn(prefetch_worker)
given_items = Cache()
postreport_queue = eventlet.Queue(POSTREPORT_QUEUE_SIZE)
postreport_worker_pool = eventlet.pools.Pool(max_size=1)
postreport_worker_pool.create = lambda: eventlet.spawn(postreport_worker)
storage_connections = eventlet.pools.TokenPool(max_size=STORAGE_MAX_CONNECTIONS)


def get_from_prefetch_queue(size):
    result = []
    while len(result) < size:
        eventlet.sleep()
        try:
            pack = prefetch_queue.get(timeout=PREFETCH_GET_TIMEOUT)
        except eventlet.queue.Empty:
            break
        result.extend(pack)
    return result

def prefetch_worker():
    while True:
        with storage_connections.item():
            docs = storage.query_meta_new_random(PREFETCH_SINGLE_LIMIT)
        if len(docs) == 0:
            eventlet.sleep(0.01)
            continue
        else:
            # Note: putting a *list* as a single item into queue
            prefetch_queue.put(docs)

def postreport_worker():
    while True: # outer forever loop
        docs = []
        while len(docs) < POSTREPORT_FLUSH_SIZE: # inner accumulator loop
            try:
                item = postreport_queue.get(timeout=POSTREPORT_FLUSH_DELAY)
                try:
                    old_doc = given_items[item['url']]
                    old_doc.update(item)
                    item = old_doc
                except KeyError:
                    pass
                docs.append(item)
            except eventlet.queue.Empty:
                break
        if not docs:
            continue
        with storage_connections.item():
            storage.update_meta(docs)

def postreport_update_worker():
    queue = postreport_state['update-queue'] # shortcut
    while True: # outer forever loop
        docs = []
        while len(docs) < POSTREPORT_FLUSH_SIZE: # inner accumulator loop
            try:
                item = queue.get(timeout=POSTREPORT_FLUSH_DELAY)
                docs.append(item)
            except eventlet.queue.Empty:
                break
        if not docs:
            continue
        with storage_connections.item():
            storage.update_meta(docs)
postreport_state['update-worker'] = eventlet.spawn(postreport_update_worker)

@log_exceptions
def crawl_queue(request):
    limit = int(request.POST['limit'])
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    time_now = datetime.datetime.now()

    with prefetch_worker_pool.item():
        pass
    doc_list = get_from_prefetch_queue(limit)
    for doc in doc_list:
        given_items.set(doc['url'], doc, 300)

    def is_old(doc):
        visited_str = doc['visited']
        if not visited_str:
            return True
        visited = dateutil.parser.parse(visited_str)
        diff = time_now - visited
        return diff > MIN_VISIT_TIMEDELTA

    doc_list = filter(is_old, doc_list)

    def make_queue_item(doc):
        filter_fields = ('url', 'headers', 'visited',)
        return dict( (k,v) for (k,v) in doc.iteritems() if k in filter_fields )

    queue = map(make_queue_item, doc_list)
    return queue

@log_exceptions
def report_result(request):
    report = cjson.decode(request.body)

    with postreport_worker_pool.item():
        pass

    links = report.pop('links', [])
    # FIXME: magic number
    if len(links) > 1000:
        log.info("Too many links: %d at %s", len(links), report['url'])

    # save reports
    if report['url']:
        content = report.pop('content', None)
        if content is not None:
            storage.save_content(settings.storage_root, report['url'], content)
        doc = storage.query_meta_by_url_one(report['url'])
        if doc is not None:
            doc.update(report)
            doc['links_count'] = len(links)
            postreport_queue.put(doc)
        else:
            print"-=============- UNEXPECTED not found doc for URL", report['url']
            # new link
            postreport_queue.put(doc)

    # put links into queue
    # 1. remove duplicates
    links = list(set(links))
    # 2. check for existing links
    def url_filter(url):
        url_lower = url.lower()
        return url_lower.startswith("http") and \
            (url.endswith("/") or url_lower.endswith("html") or url_lower.endswith("php"))

    links = filter(url_filter, links)
    for url in links:
        new_doc = {'_id': url, 'url': url, 'parent': report['url'], 'visited': None}
        postreport_queue.put(new_doc)

    return None
