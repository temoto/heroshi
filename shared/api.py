import cjson
from urllib import urlencode

from shared import REAL_USER_AGENT
from shared.conf import settings
from shared.error import ApiError
from shared.kot2 import http_request
from shared.misc import get_logger
log = get_logger()


CONTROL_SUSPEND = 'suspend'
CONTROL_RESUME = 'resume'


def request_manager(resource, method, data=None):
    headers = {
        'X-Heroshi-Auth': settings.api_key,
        'Expect': '', # a try to fix result: 100 not-ok problem
    }

    # concat URIs carefully
    base_url = settings.manager_url
    url = base_url.strip('/') + resource

    # make request
    result = http_request(method, url, data, headers=headers, user_agent=REAL_USER_AGENT)

    if not result.is_ok():
        raise ApiError("non-ok-result. Code: %s" % result.status_code)

    return result

def configure():
    r = request_manager('/worker', 'GET')
    result = cjson.decode(r.content)
    return result

def get_crawl_queue(limit):
    r = request_manager('/crawl-queue', 'POST', urlencode({'limit': limit}))
    queue = cjson.decode(r.content)
    return queue

def report_results(items):
    request_manager('/report', 'PUT', cjson.encode(items))
