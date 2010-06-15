"""Heroshi Postgres database helpers."""

from psycopg2 import DatabaseError, IntegrityError

from heroshi import get_logger
log = get_logger("storage.dbhelpers")
from heroshi.error import StorageError
from . import sql


class DbRow(object):
    def __repr__(self):
        return u"<DbRow %s>" % ", ".join("%s=%s" % (n, v) for (n,v) in self.__dict__.iteritems())


def obj_factory(columns, row):
    r = DbRow()
    r.__dict__ = dict_factory(columns, row)
    return r

def dict_factory(columns, row):
    return dict( (column, row[index]) for index, column
                 in enumerate(columns) )

def fetch(cursor, factory=None):
    """Fetches all results from cursor as *list* of `factory()` items.

    Default factory is `dict_factory`, it makes dict
    with column names as keys.

    You can also use `obj_factory`, it makes `DbRow` objects
    with column names as attributes.

    Also, you can use custom factory function like
    `factory(columns, row)`, where
    * `columns` is a tuple of column names, see Python DB-API.
    `columns` is first "column" in `cursor.description` matrix.
    * `row` is a tuple of values from DB, see Python DB-API `cursor.fetchall`.
    """
    raw_records = cursor.fetchall()
    columns = [ names[0] for names in cursor.description ]
    # Disabling duplicate column names because they cause dict (and DbRow) factories to produce shit
    # algorithm is: iterate columns backwards. if current column exists more than once, rename it
    for indx in reversed(xrange(len(columns))):
        if columns.count(columns[indx]) > 1:
            columns[indx] = '_duplicate'

    if factory is None:
        factory = dict_factory

    records = [ factory(columns, r) for r in raw_records ]
    return records

def transaction(conn, job):
    """Wraps job function into DB transaction.

    Commits on job success, reverts on exception.
    Job function must accept one argument `conn`: DB connection.
    """
    try:
        result = job(conn)
        conn.commit()
    except:
        conn.rollback()
        raise
    return result

def in_transaction(get_connection):
    """Transaction decorator for simple job functions (which accept one argument - connection).
    """
    def decorator(func):
        return transaction(get_connection(), func)
    return decorator

def add_or_change(connection, table, lookup_key, data):
    """Create new or update existing records, based on existance of lookup_key.
    """
    fetch_last_row_id = False
    if lookup_key in data: # use update
        sql_query, sql_params = sql.update(table,
                                           data,
                                           {lookup_key: data[lookup_key]})
    else: # use insert
        sql_query, sql_params = sql.insert(table, data)
        #fetch_last_row_id = True
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, sql_params)
        if cursor.rowcount == 0:
            raise StorageError("unexpected rowcount==0 at add_or_change into %s" % (table,))
        if fetch_last_row_id: # new record
            cursor.execute("SELECT currval('%s_id_seq');" % table)
            last_row_id = cursor.fetchone()[0]
            return {lookup_key: last_row_id}
        else: # updating old record
            return {lookup_key: data[lookup_key]}
    except DatabaseError, e:
        raise StorageError(unicode(e))

def post_list(connection, table, data_list):
    return [ add_or_change(connection, table, 'id', data)
             for data in data_list ]

def query(connection, table, select=None, filters=None, order_by=None,
          limit=0, offset=0, for_update=False,
          row_factory=None):
    select = select or ()
    filters = filters or {}
    sql_query, sql_params = sql.select(table,
                                       columns=select,
                                       limit=limit,
                                       offset=offset,
                                       order_by=order_by,
                                       for_update=for_update,
                                       **filters
                                       )
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, sql_params)
        return fetch(cursor, row_factory)
    except DatabaseError, e:
        raise StorageError(unicode(e))

def create(connection, table, data):
    sql_query, sql_params = sql.insert(table, data)
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, sql_params)
        if cursor.rowcount == 0:
            raise StorageError("unexpected rowcount==0 at create() into %s" % (table,))
        return True
    except IntegrityError:
        raise
    except DatabaseError, e:
        raise StorageError(unicode(e) + " with " + str(sql_params))

def update(connection, table, data, where):
    sql_query, sql_params = sql.update(table, data, where)
    cursor = connection.cursor()
    try:
        cursor.execute(sql_query, sql_params)
        return cursor.rowcount
    except DatabaseError, e:
        raise StorageError(unicode(e))

def delete(connection, table, where):
    cursor = connection.cursor()
    try:
        sql_query, sql_params = sql.delete(table, **where)
        cursor.execute(sql_query, sql_params)
        return cursor.rowcount
    except DatabaseError, e:
        raise StorageError(unicode(e))

