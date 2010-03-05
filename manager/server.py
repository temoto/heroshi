import cjson
import webob
import webob.exc
from base64 import b64encode
import hashlib

from shared.conf import settings
from shared.misc import gzip_string, init_logging, get_logger
from shared.wsgi import method_dispatcher
from manager import crawl_queue, report_results

init_logging(level=settings.get('loglevel'))
log = get_logger()


AUTH_HEADER = "X-Heroshi-Auth"


class Response(webob.Response):
    default_content_type = 'text/plain'
    default_conditional_response = True


def check_auth(request):
    auth_key = request.headers.get(AUTH_HEADER, None)
    if auth_key is None:
        return "Authentication header %s not found." % AUTH_HEADER
    if auth_key not in settings.authorized_keys:
        return "Key %s is not authorized." % auth_key
    request.api_key = auth_key


urls = {
    '/crawl-queue': method_dispatcher(post=crawl_queue),
    '/report':      method_dispatcher(put=report_results),
}

MIN_COMPRESS_LENGTH = 400

def server(request):
    handler = urls.get(request.path)
    if not handler:
        # default behaviour is 404 to all unknown URLs
        raise webob.exc.HTTPNotFound()
    auth_error = check_auth(request)
    if auth_error:
        log.info("auth: " + auth_error)
        raise webob.exc.HTTPUnauthorized(auth_error)
    result = handler(request)
    response = Response(cjson.encode(result), content_type='application/json')
    if not response.etag and (200 <= response.status_int < 300):
        # generate Etag from URL and response.body
        sha256_tag = hashlib.sha256(request.path + response.body).digest()
        response.etag = b64encode(sha256_tag)
    if 'gzip' in request.accept_encoding and len(response.body) > MIN_COMPRESS_LENGTH:
        # client supports gzipped answer and response is reasonably long for compression benefits
        response.content_encoding = 'gzip'
        response.body = gzip_string(response.body)
    return response

def wsgi_app(environ, start_response):
    request = webob.Request(environ)
    try:
        response = server(request)
    except webob.exc.HTTPError, e:
        # normal way to return 4xx or 5xx response
        response = e
    except:
        log.exception("unhandled")
        response = webob.exc.HTTPInternalServerError("really bad server error")
    response.headers['Server'] = "heroshi-manager"
    return response(environ, start_response)
