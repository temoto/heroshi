import cjson
from urllib import urlencode

from shared import REAL_USER_AGENT
from shared.conf import settings
from shared.error import ApiError
from shared.kot2 import http_request
from shared.misc import get_logger
log = get_logger()


def request(method, resource, data=None):
    headers = {
        'Expect': '', # fixes result-100-not-ok problem
    }

    # concat URIs carefully
    url = "/".join( s.strip("/") for s in (settings.couchdb_url, settings.db_name, resource) )

    # make request
    result = http_request(method, url, data, headers=headers, user_agent=REAL_USER_AGENT)

    if not result.is_ok():
        raise ApiError("non-ok-result. Code: %s" % result.status_code)

    answer = cjson.decode(result.content)

    return answer

def query(uri, post_data=None, **kwargs):
    params = {'include_docs': True}
    params.update(kwargs)

    # encode params
    params = dict( (k, cjson.encode(v)) for (k,v) in params.iteritems() )

    request_string = uri + "?" + urlencode(params)

    method = 'GET' if post_data is None else 'POST'
    answer = request(method, request_string, post_data)

    if 'rows' not in answer:
        raise ApiError("'rows' key not found in answer %s" % answer)

    return answer['rows']

def query_by_url(*urls, **params):
    if len(urls) == 0:
        return []

    return query("_design/queue/_view/by-url",
            post_data=cjson.encode({'keys': urls}),
            **params)

def query_by_url_one(url):
    results = query_by_url(url)
    return results[0] if results else None
