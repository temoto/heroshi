"""WSGI utils"""

from webob.exc import HTTPMethodNotAllowed


def method_dispatcher(default=None, **kwargs):
    """Dispatcher for switching callables by request method.

    Return values can be substituted for request handlers, e.g.:

    >>> urls = {'/foo': method_dispatcher(get=foo_get, post=foo_post,
                                          default=foo_other)}

    If `default` is supplied, it will be called for any
        request method not explicitly specified.
    If `default` is not supplied, `MethodNotAllowedError` is raised
        for any request method not explicitly specified."""

    methods = {}
    for method,handler in kwargs.iteritems():
        methods[method.upper()] = handler

    def server(request):
        handler = methods.get(request.method)
        if handler:
            return handler(request)
        if default:
            return default(request)
        raise HTTPMethodNotAllowed(''.join(methods.keys()))

    return server
