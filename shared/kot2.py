import pycurl
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


# hard maximum concurrent connections limit
HARD_MAX_CONN = 20*1000
# defaults
DEFAULT_MAX_REDIRECTS = 30
DEFAULT_CONNECT_TIMEOUT = 5*1000
DEFAULT_TIMEOUT = 40*1000


# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
# try:
#     from signal import signal, SIGPIPE, SIG_IGN
# except ImportError:
#     pass
# else:
#     pass
#     signal(SIGPIPE, SIG_IGN)


class RequestResult(object):
    """Just a namespace"""

    url = None
    result = None # simple bool - whether grabbed successfully or not
    status_code = None
    content = None

    def __init__(self, **kwargs):
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    def is_ok(self):
        return self.result and 200 <= self.status_code < 300

    def __repr__(self):
        return "<%s %d %s>" % (self.__class__.__name__, self.status_code, self.url)


def parse_header_line(line):
    """"Server: nginx" -> tuple("Server", "nginx")
    "Wrong header" -> tuple("Wrong header")"""

    bits = line.split(":", 1)
    return tuple( b.strip() for b in bits )

def parse_headers(string):
    """"Server: nginx\\r\\nEtag: foobar\\r\\n" -> {"Server": "nginx", "Etag": "foobar"}"""

    lines = string.strip().split("\r\n")
    return dict( k_v for k_v in map(parse_header_line, lines) if len(k_v) == 2 )

def grab_multi(url_list,
        headers=None,
        max_conn=HARD_MAX_CONN,
        user_agent=None,
        max_redirects=DEFAULT_MAX_REDIRECTS,
        connect_timeout=DEFAULT_CONNECT_TIMEOUT,
        timeout=DEFAULT_TIMEOUT,
        ):
    """Perform multiple asynchronous http GET requests.
    url_list is list of (url,referer) tuples. referer can be None.
    user_agent as argument takes precedence of User-Agent in headers.

    Returns dict {url: kot2.HttpResponse} with all relevant attributes."""

    if not url_list:
        # empty url list -> empty result list
        return {}

    # Make a queue with (url, referer) tuples.
    queue = []
    for url,referer in url_list:
        url = url.strip()
        if not url:
            continue
        queue.append((url, referer))

    # Check args
    num_urls = len(queue)
    num_conn = min(max_conn, num_urls)
    assert 1 <= num_conn <= HARD_MAX_CONN, "invalid number of concurrent connections: %s" % num_conn

    # Pre-allocate a list of curl objects
    m = pycurl.CurlMulti()
    m.handles = []
    for _ in range(num_conn):
        c = pycurl.Curl()
        c.store_body = StringIO()
        c.store_headers = StringIO()
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, max_redirects)
        c.setopt(pycurl.CONNECTTIMEOUT_MS, connect_timeout)
        c.setopt(pycurl.TIMEOUT_MS, timeout)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.WRITEFUNCTION, c.store_body.write)
        c.setopt(pycurl.HEADERFUNCTION, c.store_headers.write)
        c.setopt(pycurl.NOPROGRESS, 1)
        if headers is None:
            headers = {}
        if user_agent is not None:
            headers['User-Agent'] = user_agent
        custom_headers = [ "%s: %s" % (k,v) for k,v in headers.iteritems() ]
        c.setopt(pycurl.HTTPHEADER, custom_headers)
        m.handles.append(c)

    # Main loop
    freelist = m.handles[:]
    num_processed = 0
    while num_processed < num_urls:
        # If there is an url to process and a free curl object, add to multi stack
        while queue and freelist:
            url, referer = queue.pop(0)
            c = freelist.pop()
            c.setopt(pycurl.URL, url)
            if referer:
                c.setopt(pycurl.REFERER, referer)
            m.add_handle(c)
            # store some info
            c.url = url
        # Run the internal curl state machine for the multi stack
        while True:
            ret, _ = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the freelist
        while True:
            num_q, ok_list, err_list = m.info_read()
            for c in ok_list:
                m.remove_handle(c)
                c.success = True
                freelist.append(c)
            for c, errno, errmsg in err_list:
                m.remove_handle(c)
                c.success = False
                c.error = (errno, errmsg)
                freelist.append(c)
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        m.select(1.0)

    # Cleanup
    results = {}
    for c in m.handles:
        r = RequestResult(
            url=c.url,
            result=c.success,
            status_code=c.getinfo(pycurl.RESPONSE_CODE),
            content=c.store_body.getvalue(),
            content_type=c.getinfo(pycurl.CONTENT_TYPE),
# added in 7.19.4
#             condition_unmet=c.getinfo(pycurl.CONDITION_UNMET),
            effective_url=c.getinfo(pycurl.EFFECTIVE_URL),
            redirect_count=c.getinfo(pycurl.REDIRECT_COUNT),
            total_time=c.getinfo(pycurl.TOTAL_TIME),
            connect_time=c.getinfo(pycurl.CONNECT_TIME),
            )
        r.headers = parse_headers(c.store_headers.getvalue())
        results[c.url] = r
        # we can only close Curl object after all getinfo() calls done
        c.close()
    m.close()
    return results

def http_request(method, url,
        data=None,
        headers=None,
        user_agent=None,
        max_redirects=DEFAULT_MAX_REDIRECTS,
        connect_timeout=DEFAULT_CONNECT_TIMEOUT,
        timeout=DEFAULT_TIMEOUT):
    """Perform one synchronous http request.
    user_agent as argument takes precedence of User-Agent in headers.

    Returns kot2.HttpResponse with all relevant attributes"""

    # init curl object
    c = pycurl.Curl()
    # common settings
    c.store_headers = StringIO()
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.MAXREDIRS, max_redirects)
    c.setopt(pycurl.CONNECTTIMEOUT_MS, connect_timeout)
    c.setopt(pycurl.TIMEOUT_MS, timeout)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.HEADERFUNCTION, c.store_headers.write)
    c.setopt(pycurl.NOPROGRESS, 1)
    if headers is None:
        headers = {}
    if user_agent is not None:
        headers['User-Agent'] = user_agent
    custom_headers = [ "%s: %s" % (k,v) for k,v in headers.iteritems() ]
    c.setopt(pycurl.HTTPHEADER, custom_headers)

    c.setopt(pycurl.URL, url)
    # parse method
    method = method.upper()
    if method == 'GET':
        c.setopt(pycurl.HTTPGET, 1)
    elif method == 'HEAD':
        c.setopt(pycurl.NOBODY, 1)
    elif method == 'POST':
        c.setopt(pycurl.POST, 1)
        if data is None:
            c.setopt(pycurl.POSTFIELDSIZE, 0)
        else:
            c.setopt(pycurl.POSTFIELDS, data)
            c.setopt(pycurl.POSTFIELDSIZE, len(data))
    elif method == 'PUT':
        c.setopt(pycurl.UPLOAD, 1)
        c.read_data = StringIO(data)
        c.setopt(pycurl.READFUNCTION, c.read_data.read)
        if data is None:
            c.setopt(pycurl.INFILESIZE, 0)
        else:
            c.setopt(pycurl.INFILESIZE, len(data))
    # unless request is HEAD, we're interested in response body
    if method != 'HEAD':
        c.store_body = StringIO()
        c.setopt(pycurl.WRITEFUNCTION, c.store_body.write)

    # Run the curl request
    try:
        c.perform()
        c.success = True
    except Exception:
        c.success = False

    # Cleanup
    result = RequestResult(
        result=True,
        status_code=c.getinfo(pycurl.RESPONSE_CODE),
        content=c.store_body.getvalue(),
        content_type=c.getinfo(pycurl.CONTENT_TYPE),
# added in 7.19.4
#         condition_unmet=c.getinfo(pycurl.CONDITION_UNMET),
        effective_url=c.getinfo(pycurl.EFFECTIVE_URL),
        redirect_count=c.getinfo(pycurl.REDIRECT_COUNT),
        total_time=c.getinfo(pycurl.TOTAL_TIME),
        connect_time=c.getinfo(pycurl.CONNECT_TIME),
        )
    result.headers = parse_headers(c.store_headers.getvalue())
    # we can only close Curl object after all getinfo() calls done
    c.close()
    return result


if __name__ == "__main__":
    sample_urls = [
#         ('http://www.google.com/', None),
#         ('http://dosug.tel/', None),
#         ('http://ya.ru/', None),
#         ('http://reggi.ru/', None),
        ('http://spamobzor.ru/', None),
        ('http://tabulaterrae.com/', None),
        ('http://temoto.ru.local/blog/trtr', None),
        ('http://temoto.ru/p/', None),
        ('http://localhost/docs', None),
        ('http://localhost/docs/down/', None),
        ('http://localhost/docs/down/viz.jpg', None),
        ('http://localhost:8293/sert', None),
    ]
    results = grab_multi(sample_urls, headers={'X-Ola': 'ala'})
    for url,result in results.iteritems():
        print "-",url
        print " ",' '.join([ "%s" % v for (k,v) in result.__dict__.iteritems() if k not in ('headers', 'content', 'effective_url') ])
