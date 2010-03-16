import cjson
import eventlet, eventlet.pools, eventlet.wsgi
import webob
import webob.exc
from base64 import b64encode
import hashlib

from heroshi.conf import settings
from heroshi.misc import gzip_string, get_logger
from heroshi.wsgi import method_dispatcher
log = get_logger("manager.server")
from .manager import Manager


AUTH_HEADER = "X-Heroshi-Auth"
manager_pool = eventlet.pools.Pool(max_size=1)
manager_pool.create = Manager


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
    '/crawl-queue': dict(POST='crawl_queue'),
    '/report':      dict(PUT='report_result'),
}

MIN_COMPRESS_LENGTH = 400

def server(request):
    handler_map = urls.get(request.path)
    if not handler_map:
        # default behaviour is 404 to all unknown URLs
        raise webob.exc.HTTPNotFound()

    auth_error = check_auth(request)
    if auth_error:
        log.info("auth: " + auth_error)
        raise webob.exc.HTTPUnauthorized(auth_error)

    with manager_pool.item() as manager:
        manager.active = True
        handler = method_dispatcher(**dict( (method, getattr(manager, name))
                                            for method,name in handler_map.iteritems() ))
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
