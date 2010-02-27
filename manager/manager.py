import cjson
import datetime
import dateutil.parser
import random
import time
import pprint

from shared import api
from shared import storage
from shared import TIME_FORMAT
from shared.conf import settings
from shared.misc import os_path_expand, init_logging, log_exceptions
log = init_logging(level=settings.get('loglevel'))


MAX_LIMIT = 1000
MIN_VISIT_TIMEDELTA = datetime.timedelta(hours=6)


@log_exceptions
def worker_config(request):
    return {
            'control': api.CONTROL_RESUME,
            'recheck-interval': 90,
           }

@log_exceptions
def crawl_queue(request):
    limit = int(request.POST['limit'])
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    time_now = datetime.datetime.now()

    # TODO: make it faster
    doc_list = storage.query_meta_not_given(MAX_LIMIT)

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

    for doc in doc_list:
        doc['given'] = time_now.strftime(TIME_FORMAT)

    claim_results = storage.update_meta(doc_list)
    claimed_docs = [ r for r in claim_results if '_id' in r ]

    def make_queue_item(doc):
        filter_fields = ('url', 'headers', 'visited',)
        return dict( (k,v) for (k,v) in doc.iteritems() if k in filter_fields )

    queue = [ make_queue_item(doc) for doc in claimed_docs ]

    return queue

@log_exceptions
def report_results(request):
    items = cjson.decode(request.body)

    for report in items:
        links = report.pop('links', [])
        if len(links) > 1000:
            print"........................ too many links\n",pprint.pformat(links)

        # save reports
        if report['url']:
            content = report.pop('content', None)
            if content is not None:
                storage.save_content(settings.storage_root, report['url'], content)
            doc = storage.query_meta_by_url_one(report['url'])
            if doc is not None:
                doc.update(report)
                doc['given'] = None
                doc['links_count'] = len(links)
                if not storage.save_meta(doc, raise_conflict=False):
                    continue
            else:
                print"-=============- UNEXPECTED not found doc for URL", report['url']
                # new link
                storage.save_meta(report, raise_conflict=False)

        # put links into queue
        # 1. remove duplicates
        links = list(set(links))
        # 2. check for existing links
        def url_filter(u):
            return u.startswith("http") and \
                (u.endswith("/") or u.endswith("html"))

        links = filter(url_filter, links)

        def make_new_doc(url):
            return {'_id': url, 'url': url, 'parent': report['url'], 'visited': None}

        new_docs = map(make_new_doc, links)
        storage.update_meta(new_docs)

    return None
