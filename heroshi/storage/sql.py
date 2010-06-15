# coding: utf-8
"""SQL builder (Postgres quoting).
"""
import re

from .safestring import SafeUnicode


class SafeSQL(SafeUnicode):
    pass


def mark_safe(string):
    """Mark string as safe SQL."""
    return SafeSQL(string)

def is_safe(value):
    """Tests if string is safe to use as SQL expression."""
    return isinstance(value, SafeSQL)

def escape(text):
    """Ensures safe SQL identifier.

    String is enclosed in double quotes "like this". Double quote character is doubled.
    If string contains dot(s), escapes dot-separated parts and clues back with dot.
    If string contains \0 byte, ValueError is raised.

    Idempotent.

    Examples::

        >>> print sql.escape('foo-bar')
        "foo-bar"
        >>> print sql.escape('hit . visitor_id')
        "hit"."visitor_id"
        >>> print sql.escape('hit.when')
        "hit"."when"
    """
    if is_safe(text):
        return text
    if text.isdigit():
        return mark_safe(text)
    if '\0' in text:
        raise ValueError("\\0 byte is invalid in SQL identifiers. Wrong identifier: %s" % repr(text))
    if '.' in text:
        return mark_safe('.'.join( escape(p.strip()) for p in text.split('.') ))
    return mark_safe('"' + text.replace('"', '""') + '"')

def exec_sql_file(connection, path):
    """Loads SQL file from `path` and executes it on `connection`"""
    with open(path, 'r') as f:
        sql_content = f.read()
        connection.cursor().execute(sql_content)

def as_(value, alias):
    return mark_safe(escape(value) + " AS " + escape(alias))

def with_as(func):
    def wrapper(*args, **kwargs):
        alias = kwargs.pop('as_', None)
        result = func(*args, **kwargs)
        if alias:
            return as_(result, alias)
        else:
            return result
    return wrapper

@with_as
def extract(part, value=None):
    if value is None:
        return lambda v: extract(part, v)
    else:
        return mark_safe("EXTRACT(%s FROM %s)" % (part, escape(value)))

@with_as
def date_trunc(trunc_till, value=None):
    if value is None:
        return lambda v: date_trunc(trunc_till, v)
    else:
        return mark_safe("DATE_TRUNC('%s', %s)" % (trunc_till, escape(value)))

@with_as
def sum_(value):
    return mark_safe("SUM(%s)" % escape(value))

@with_as
def count(value):
    return mark_safe("COUNT(%s)" % escape(value))

@with_as
def lower(value):
    return mark_safe("LOWER(%s)" % escape(value))

@with_as
def upper(value):
    return mark_safe("UPPER(%s)" % escape(value))

@with_as
def coalesce(*args):
    return mark_safe("COALESCE(%s)" % ", ".join( escape(a) for a in args ))

COLUMN_CODES = {'year':  extract('year'),
                'month': extract('month'),
                'day':   extract('day'),
                'trunc_year':  date_trunc('year'),
                'trunc_month': date_trunc('month'),
                'trunc_day':   date_trunc('day'),
                'trunc_hour':  date_trunc('hour'),
                'trunc_minute':date_trunc('minute'),
                'sum':   sum_,
                'count': count,
                'lower': lower,
                'upper': upper,
               }

def parse_column_codes(string, keep_original_name=False):
    """Parses double-underscore-delimited codes into SQL columns.

    Examples:
    >>> print sql.process_column("foo")
    "foo"
    >>> print sql.process_column("foo__count")
    COUNT("foo")
    >>> print sql.process_column("foo__trunc_day")
    DATE_TRUNC(\'day\', "foo")
    >>> print sql.process_column("foo__count__trunc_day")
    COUNT(DATE_TRUNC(\'day\', "foo"))
    """

    if is_safe(string):
        return string

    column = None
    result = None
    if '__' in string:
        tokens = string.split('__')
        column = escape(tokens[0])

        # chain column codes functions
        result = column
        for code in reversed(tokens[1:]):
            func = COLUMN_CODES.get(code)
            if func is None:
                raise ValueError("sql: unknown column code: %s" % code)
            result = func(result)
    else:
        result = escape(string)
    if keep_original_name and result != string:
        return as_(result, string)
    return mark_safe(result)

def where(**conditions):
    """Builds SQL WHERE statement.
    Conditions have the following format:

      a=10 <- a = 10
      a__eq=10 <- a = 10
      a__in=[1,2,3] <- a IN (1,2,3)
      a__lt=10 <- a < 10
      a__gt=10 <- a > 10
      a__starts='foo' <- a LIKE 'foo%'
      a__ends='bar' <- a LIKE '%bar'
      a__like='zar' <- a LIKE '%zar%'

    You can also use any `COLUMN_CODES` before conditions. Example::

        >>> print sql.where(where__count__lower=2)
        (u'WHERE (COUNT(LOWER("where")) = %s)', [2])

    Returns tuple (WHERE statement, list of parameters).
    """
    params = []
    parts = []
    operators = {'in': "IN",
                 'gt': ">",
                 'gte': '>=',
                 'lt': "<",
                 'lte': '<=',
                 'eq': "=",
                 'ne': "!=",
                 'starts': 'starts',
                 'ends': 'ends',
                 'like': 'like',
                 }
    for key, value in conditions.iteritems():
        operator = '='
        if '__' in key:
            maybe_key, maybe_op = key.rsplit('__', 1)
            if maybe_op in operators:
                # operator is known
                operator = operators[maybe_op]
                key = maybe_key
                # else: that's not an operator, just a wierd key with double underscores

        column_code = parse_column_codes(key)

        null_check = "(%s IS NULL)" % column_code
        if isinstance(value, (list, tuple)):
            # copy value since we may modify it
            value = value[:]
            # did we find None in value?
            is_none_found = False
            while None in value:
                # remove all None-s from list (in strange case there are many None-s), add null check
                value.remove(None)
                is_none_found = True
            if value:
                if operator in ('starts', 'ends', 'like'):
                    # FIXME: write why?
                    raise NotImplemented()
                else:
                    list_check = "(%s IN (%s))" % (column_code, ", ".join(["%s"]*len(value)))
                if is_none_found:
                    list_check = "(%s OR %s)" % (null_check, list_check)
                parts.append(list_check)
                # add list of values to parameters list
                params.extend(value)
            else:
                parts.append(null_check)
        elif value is None:
            parts.append(null_check)
        else:
            if operator == 'starts':
                parts.append("(%s LIKE %%s)" % column_code)
                value = value+'%'
            elif operator == 'ends':
                parts.append("(%s LIKE %%s)" % column_code)
                value = '%'+value
            elif operator == 'like':
                parts.append("(%s LIKE %%s)" % column_code)
                value = '%'+value+'%'
            else:
                parts.append("(%s %s %%s)" % (column_code, operator))
            params.append(value)
    sql_query = "WHERE " + " AND ".join(parts)
    return (sql_query, params)

def insert(table, data):
    parts = []
    params = []
    parts.append("INSERT INTO " + escape(table))
    columns = []
    for column, value in data.iteritems():
        columns.append(escape(column))
        params.append(value)
    parts.append("("+ ", ".join(columns) +")")
    parts.append("VALUES (" + ", ".join(["%s"]*len(columns)) + ")")
    query = " ".join(parts) + ";"
    return (query, params)

def update(table, data, conditions={}):
    parts = []
    params = []
    parts.append("UPDATE " + escape(table))
    parts.append("SET")
    set_data = []
    for column, value in data.iteritems():
        set_data.append(escape(column) + "=%s")
        params.append(value)
    parts.append(", ".join(set_data))
    if conditions:
        where_query, where_params = where(**conditions)
        parts.append(where_query)
        params += where_params
    query = " ".join(parts) + ";"
    return (query, params)

def delete(table, **conditions):
    parts = []
    params = []
    parts.append("DELETE FROM " + escape(table))
    where_query, where_params = where(**conditions)
    parts.append(where_query)
    params += where_params
    query = " ".join(parts) + ';'
    return (query, params)

def subquery(source, **kwargs):
    """Builds SQL SELECT subquery suitable for select() source argument to build nested queries.
    """
    alias = kwargs.pop('as_', None)
    if alias is None:
        # TODO: generate alias
        raise TypeError("subquery() requires keyword argument as_ specifying alias for subquery")

    query, params = select(source, **kwargs)
    query = mark_safe("(" + query.rstrip(';') + ")" + " AS " + escape(alias))
    return (query, params)

def _join(join_type, source, condition):
    """Builds SQL JOIN part for SELECT query.

    Arguments:
        - join_type: string type of join. Can be either of 'INNER', 'LEFT', 'RIGHT', 'FULL'
        - source: see select()
        - condition: safe SQL with join condition. For example: mark_safe("col1 = col2")
            Use escape() when in doubt whether plain column name is safe.

    Basically, produces such safe SQL code::

        join_type JOIN "source" ON condition
    """
    sql_source, params = _parse_source(source)
    if not is_safe(condition):
        raise TypeError("join_condition argument must be safe SQL. Use mark_safe() to mark your string as safe SQL and take responsibility.")
    return (mark_safe("%s JOIN %s ON %s" % (join_type, escape(sql_source), condition)),
            params)

def inner_join(source, condition):
    """Builds SQL INNER JOIN part for SELECT query.

    Arguments:
        - source: see select()
        - condition: safe SQL with join condition. For example: mark_safe("col1 = col2")
            Use escape() when in doubt whether plain column name is safe.

    Basically, produces such safe SQL code::

        INNER JOIN "source" ON condition
    """
    return _join('INNER', source, condition)

def left_join(source, condition):
    """Builds SQL LEFT JOIN part for SELECT query. See inner_join()."""

    return _join('LEFT', source, condition)

def right_join(source, condition):
    """Builds SQL RIGHT JOIN part for SELECT query. See inner_join()."""

    return _join('RIGHT', source, condition)

def full_join(source, condition):
    """Builds SQL FULL JOIN part for SELECT query. See inner_join()."""

    return _join('FULL', source, condition)

def _parse_source(source):
    if isinstance(source, basestring):
        # escape table name
        return (escape(source), [])
    else:
        # leave subquery unescaped
        try:
            sql_query, params = source
        except (TypeError, ValueError):
            raise TypeError("_parse_source() failed parsing <%s %s> as non-basestring" % (type(source), repr(source)))
        return (sql_query, params)

def select(source, columns=[], join=None, group_by=None, order_by=None, limit=0, offset=0, for_update=False, **conditions):
    """
    Builds SQL SELECT query.
    Returns tuple (query string, list of parameters).

    Arguments:
     - source: either string - name of a table to select from
                   or tuple(query,params) - result from another select() call
     - columns: list of strs - list of columns to select
     - join: tuple(query,params). Use apropriate functions: inner/left/right/full _join()
                                  to generate this value.
     - limit: int - limit amount of returned rows
     - offset: int - offset from start of returned rows
     - order_by: list of strs - order by specified columns. DESC is set by prepending column
                                with minus sign (-)
     - group_by: list of columns used in GROUP BY statement
     - as_: alias for subquery. as_='foo' makes (query) AS "foo" !NOTE! without tailing semicolon
     - conditions: see help for `where`

    Examples::

        >>> print sql.select(sql.subquery('hit', as_='t'))
        ('SELECT * FROM (SELECT * FROM "hit") AS "t";', [])
    """
    parts = ["SELECT"]
    params = []

    select_from, source_params = _parse_source(source)
    params.extend(source_params)

    if columns:
        parts.append(", ".join( escape(column) for column in columns ))
    else:
        parts.append("*")
    parts.append("FROM")
    parts.append(select_from)

    if join:
        join_sql, join_params = join
        parts.append(join_sql)
        params.extend(join_params)
    if conditions:
        where_query, where_params = where(**conditions)
        parts.append(where_query)
        params += where_params
    if group_by:
        group_parts = []
        parts.append("GROUP BY")
        for col in group_by:
            group_parts.append(escape(col))
        parts.append(", ".join(group_parts))
    if order_by:
        order_parts = []
        parts.append("ORDER BY")
        for col in order_by:
            if col[0] == "-":
                order_parts.append(escape(col[1:]) + " DESC")
            else:
                order_parts.append(escape(col) + " ASC")
        parts.append(", ".join(order_parts))
    if limit:
        parts.append("LIMIT %d" % limit)
    if offset:
        parts.append("OFFSET %d" % offset)
    if for_update:
        parts.append("FOR UPDATE NOWAIT")
    query = " ".join(parts) + ";"
    return (query, params)
