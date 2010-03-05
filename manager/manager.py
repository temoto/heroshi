import cjson
import datetime
import dateutil.parser
import random

from shared import storage
from shared.conf import settings
from shared.misc import init_logging, log_exceptions
log = init_logging(level=settings.get('loglevel'))


MAX_LIMIT = 1000
MIN_VISIT_TIMEDELTA = datetime.timedelta(hours=6)

NEW_URLS = set()


@log_exceptions
def crawl_queue(request):
    limit = int(request.POST['limit'])
    if limit > MAX_LIMIT:
        limit = MAX_LIMIT

    time_now = datetime.datetime.now()

    # TODO: make it faster
    doc_list = storage.query_meta_new(MAX_LIMIT)

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

    if len(queue) < limit:
        some_new_urls = list(NEW_URLS)[:10000]
        random.shuffle(some_new_urls)
        random_slice = some_new_urls[:limit - len(queue)]
        NEW_URLS.difference_update(random_slice)
        queue.extend( {'url': url} for url in random_slice )

    return queue

@log_exceptions
def report_results(request):
    items = cjson.decode(request.body)

    for report in items:
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
            ul = u.lower()
            return ul.startswith("http") and \
                (u.endswith("/") or ul.endswith("html") or ul.endswith("php"))

        links = filter(url_filter, links)

        NEW_URLS.update(links)

        def make_new_doc(url):
            return {'_id': url, 'url': url, 'parent': report['url'], 'visited': None}

        new_docs = map(make_new_doc, links)
        storage.update_meta(new_docs)

    return None
