"""Heroshi URL server implementation main module."""

__all__ = ['crawl_queue', 'report_result']

import cjson
import datetime
import dateutil.parser
import random

from heroshi import storage
from heroshi.conf import settings
from heroshi.misc import get_logger, log_exceptions
log = get_logger("manager")


MAX_LIMIT = 1000
MIN_VISIT_TIMEDELTA = datetime.timedelta(hours=6)


@log_exceptions
def crawl_queue(request):
    limit = int(request.POST['limit'])
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    time_now = datetime.datetime.now()

    # TODO: make it faster
    doc_list = storage.query_meta_new_random(MAX_LIMIT)

    def is_old(doc):
        visited_str = doc['visited']
        if not visited_str:
            return True
        visited = dateutil.parser.parse(visited_str)
        diff = time_now - visited
        return diff > MIN_VISIT_TIMEDELTA

    doc_list = filter(is_old, doc_list)

    random.shuffle(doc_list)
    doc_list = doc_list[:limit]

    def make_queue_item(doc):
        filter_fields = ('url', 'headers', 'visited',)
        return dict( (k,v) for (k,v) in doc.iteritems() if k in filter_fields )

    queue = map(make_queue_item, doc_list)
    return queue

@log_exceptions
def report_result(request):
    report = cjson.decode(request.body)

    links = report.pop('links', [])
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
            if not storage.save_meta(doc, raise_conflict=False):
                return None
        else:
            print"-=============- UNEXPECTED not found doc for URL", report['url']
            # new link
            storage.save_meta(report, raise_conflict=False)

    # put links into queue
    # 1. remove duplicates
    links = list(set(links))
    # 2. check for existing links
    def url_filter(url):
        url_lower = url.lower()
        return url_lower.startswith("http") and \
            (url.endswith("/") or url_lower.endswith("html") or url_lower.endswith("php"))

    links = filter(url_filter, links)

    def make_new_doc(url):
        return {'_id': url, 'url': url, 'parent': report['url'], 'visited': None}

    new_docs = map(make_new_doc, links)
    storage.update_meta(new_docs)

    return None
