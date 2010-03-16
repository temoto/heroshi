# coding: utf-8

__all__ = ['StorageConnection']

import couchdbkit as couchdb
import random

from heroshi.conf import settings
from heroshi.misc import get_logger
log = get_logger("storage")


class StorageConnection(object):
    def __init__(self):
        self._server = couchdb.Server(settings.storage['couchdb_url'])
        self._db = self._server[settings.storage['db_name']]

    def save_content(self, doc, content, content_type):
        if doc.get('_attachments', {}).get("content", {}).get("length", -1) == len(content):
            log.debug(u"Skipping update with same length.")
            return
        self._db.put_attachment(doc, content, name="content", content_type=content_type)

    def get_content(self, doc=None, url=None):
        raise NotImplementedError()

    def save(self, data, raise_conflict=True, force_update=False, batch='ok'):
        data['_id'] = data['url']
        try:
            res = self._db.save_doc(data, force_update=force_update, batch=batch)
            data['_rev'] = res['rev']
            return True
        except couchdb.ResourceConflict:
            if raise_conflict:
                raise
        except couchdb.RequestFailed:
            log.exception(u"save")

    def update(self, items, raise_conflict=True, all_or_nothing=False, ensure_commit=False):
        try:
            update_results = self._db.bulk_save(items, use_uuids=False, all_or_nothing=all_or_nothing)
            if ensure_commit:
                self._db.ensure_full_commit()
            return update_results
        except couchdb.BulkSaveError:
            if raise_conflict:
                raise
        except couchdb.RequestFailed:
            log.exception(u"update")

    def _query_view(self, view, limit, **params):
        result_gen = self._db.view(view, include_docs=True, limit=limit, **params)
        return [ r['doc'] for r in result_gen ]

    def query_all_by_url(self, url, limit=1, stale='ok', **params):
        return self._query_view("all/by-url", limit, startkey=url, stale=stale, **params)

    def query_all_by_url_one(self, url):
        results = self.query_all_by_url(url=None, limit=1, key=url)
        return results[0] if results else None

    def query_new_random(self, limit=None):
        return self._query_view("queue/new-random", limit, startkey=random.random(), stale='ok')

    def query_visited_by_url(self, url=None, limit=None, stale='ok', **params):
        return self._query_view("visited/by-url", limit, startkey=url, stale=stale, **params)
