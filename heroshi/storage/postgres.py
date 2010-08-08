# coding: utf-8
"""PostgreSQL storage backend for Heroshi."""

__all__ = ['StorageConnection']

from datetime import datetime
from itertools import imap
from functools import partial
import json
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

from heroshi import TIME_FORMAT, get_logger
log = get_logger("storage.postgres")
from heroshi.conf import settings
from heroshi.error import StorageError
from . import dbhelpers, sql


RANDOMIZER_K = 50
RECHECK_INTERVAL = '2 days'
TABLE = 'metadata'


def row_factory(columns, values):
    row = dbhelpers.dict_factory(columns, values)
    row.update(json.loads(row.pop('var') or "{}"))
    row['headers'] = json.loads(row['headers'] or "{}")
    return row

def item_to_row(item):
    dup = dict(item)
    row = {}
    row['url'] = dup.pop('url')
    visited = dup.pop('visited')
    row['visited'] = datetime.strptime(visited, TIME_FORMAT) if visited else None
    row['result'] = dup.pop('result', None)
    row['fetch_time'] = dup.pop('fetch_time', None)
    row['status_code'] = dup.pop('status_code', None)

    headers = dup.pop('headers', {})
    row['headers'] = json.dumps(headers) if headers else None

    var = {}
    if 'parent' in dup:    var['parent'] = dup.pop('parent')
    if 'redirects' in dup: var['redirects'] = json.dumps(dup.pop('redirects'))
    row['var'] = json.dumps(var) if var else None

    if dup:
        raise ValueError("Item %s contains unexpected fields: %s." % (row['url'], dup.keys()))
    return row

def save_job(conn, row):
    filters = {'url__eq': row['url']}

    r = dbhelpers.query(conn, TABLE, filters=filters, for_update=True)
    if len(r) == 0:
        # no such row
        dbhelpers.create(conn, TABLE, row)
    else:
        dbhelpers.update(conn, TABLE, row, filters)


class StorageConnection(object):
    def __init__(self):
        try:
            self._conn = psycopg2.connect(settings.storage['postgres_dsn'])
        except psycopg2.DatabaseError, e:
            # TODO: handle
            log.error("postgres connect error type: %s", type(e))
            raise
        self._conn.set_client_encoding('UNICODE')

    def save_content(self, item, content, content_type):
        """TODO
        """
        def job(conn):
            filters = {'url__eq': item['url']}

            r = dbhelpers.query(conn, TABLE, filters=filters, for_update=True)
            if len(r) == 0:
                # no such row
                raise StorageError("save_content on non-existent url: %s", item['url'])
            else:
                dbhelpers.update(conn,
                                 TABLE,
                                 {'content': content,
                                  'content_type': content_type,
                                  'content_length': len(content),
                                 },
                                 filters)

        dbhelpers.transaction(self._conn, job)

    def get_content(self, doc=None, url=None):
        """TODO
        """
        raise NotImplementedError()

    def save(self, data):
        """TODO
        """
        row = item_to_row(data)
        dbhelpers.transaction(self._conn, partial(save_job, row=row))
        return True

    def update(self, items):
        """TODO
        """
        if not items:
            return []

        def job(conn):
            rows = imap(item_to_row, items)
            results = [ save_job(conn, row) for row in rows ]
            return results
        return dbhelpers.transaction(self._conn, job)

    def query_new_random(self, limit):
        """TODO
        """
        def job(conn):
            cursor = conn.cursor()
            sql = """
select {columns} from
   (select {columns} from "{table}"
    where ("visited" is null) or ("visited" < now() - interval %s)
    order by "visited" nulls first
    limit %s) as "big_query"
order by random()
limit %s;
""".format(columns='"url", "visited", "headers", "var"',
                    table=TABLE)
            params = (RECHECK_INTERVAL, limit*RANDOMIZER_K, limit)
            #log.debug("SQL: " + sql, *params)
            cursor.execute(sql, params)
            # fetch everything
            cursor.arraysize = cursor.rowcount
            return dbhelpers.fetch(cursor, factory=row_factory)
        results = dbhelpers.transaction(self._conn, job)
        log.info(u"Got %d items to queue.", len(results))
        return results

