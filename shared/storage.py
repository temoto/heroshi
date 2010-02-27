from __future__ import with_statement
import os
import hashlib
import couchdb.client
import pprint

from shared.conf import settings
from shared.misc import os_path_expand, get_logger
log = get_logger()


def get_hash_path(root, string):
    hash_ = hashlib.sha1(string).hexdigest()
    path = os.path.join(root, hash_[:2], hash_[2:4], hash_[4:])
    path = os_path_expand(path)
    return path

def save_content(root, url, content):
    path = get_hash_path(root, url)
    path_dir = os.path.dirname(path)
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)
    with open(path, 'wb') as f:
        f.write(content.encode('utf-8'))

def save_meta(data, raise_conflict=True):
    server = couchdb.client.Server(settings.couchdb_url)
    db = server['heroshi']
    id_ = data['url']
    try:
        db[id_] = data
        return True
    except couchdb.client.ResourceConflict:
        if raise_conflict:
            raise
    except couchdb.client.ServerError:
        log.exception("")

def update_meta(items):
    server = couchdb.client.Server(settings.couchdb_url)
    db = server['heroshi']
    try:
        r = db.update(items)
        return list(r)
    except couchdb.client.ResourceConflict:
        pass
#         log.error("resource conflict for items", items)
    except couchdb.client.ServerError:
        log.exception("")

def _query_meta_view(view, limit, **kwargs):
    server = couchdb.client.Server(settings.couchdb_url)
    db = server['heroshi']

    params = {'include_docs': True}
    params.update(kwargs)
    if limit:
        params['limit'] = limit

    result_gen = db.view(view, **params)
    return [ r.doc for r in result_gen ]

def query_meta_by_url(url, limit=1):
    return _query_meta_view("_design/queue/_view/by-url", limit, key=url)

def query_meta_by_url_one(url):
    results = query_meta_by_url(url, 1)
    return results[0] if results else None

def query_meta_not_given(limit=None):
    return _query_meta_view("_design/queue/_view/not-given", limit)
