import cjson
import httplib2
from urllib import urlencode

from shared import REAL_USER_AGENT
from shared.conf import settings
from shared.error import ApiError
from shared.misc import get_logger
log = get_logger()


def request_manager(resource, method, data=None, headers=None):
    use_headers = {
        'User-Agent': REAL_USER_AGENT,
        'X-Heroshi-Auth': settings.api_key,
        'Expect': '', # a try to fix result: 100 not-ok problem
    }
    if headers is not None:
        use_headers.update(headers)

    # concat URIs carefully
    base_url = settings.manager_url
    url = base_url.strip('/') + resource

    # make request
    http = httplib2.Http()
    response, content = http.request(url, method, body=data, headers=use_headers)

    if not (200 <= response.status < 300):
        raise ApiError("non-ok-result. Code: %s" % response.status)

    return content

def get_crawl_queue(limit):
    response = request_manager('/crawl-queue', 'POST',
                               urlencode({'limit': limit}),
                               {'Content-Type': "application/x-www-form-urlencoded"})
    queue = cjson.decode(response)
    return queue

def report_results(items):
    request_manager('/report', 'PUT', cjson.encode(items))
