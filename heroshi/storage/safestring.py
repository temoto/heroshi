# coding: utf-8
"""SafeString is compatible with regular str,
bug only keeps "safety" when concatenated with another SafeString.

Usage is to provide automatic control on escaping of HTML, SQL, XML, etc languages.
"""
# from Django
# http://djangoproject.com/


def curry(_curried_func, *args, **kwargs):
    def _curried(*moreargs, **morekwargs):
        return _curried_func(*(args+moreargs), **dict(kwargs, **morekwargs))
    return _curried


class SafeData(object):
    pass


class SafeString(str, SafeData):
    """
    A string subclass that has been specifically marked as "safe" (requires no
    further escaping) for HTML output purposes.
    """
    def __add__(self, rhs):
        """
        Concatenating a safe string with another safe string or safe unicode
        object is safe. Otherwise, the result is no longer safe.
        """
        t = super(SafeString, self).__add__(rhs)
        if isinstance(rhs, SafeUnicode):
            return SafeUnicode(t)
        elif isinstance(rhs, SafeString):
            return SafeString(t)
        return t

    def _proxy_method(self, *args, **kwargs):
        """
        Wrap a call to a normal unicode method up so that we return safe
        results. The method that is being wrapped is passed in the 'method'
        argument.
        """
        method = kwargs.pop('method')
        data = method(self, *args, **kwargs)
        if isinstance(data, str):
            return SafeString(data)
        else:
            return SafeUnicode(data)

    decode = curry(_proxy_method, method=str.decode)


class SafeUnicode(unicode, SafeData):
    """
    A unicode subclass that has been specifically marked as "safe" for HTML
    output purposes.
    """
    def __add__(self, rhs):
        """
        Concatenating a safe unicode object with another safe string or safe
        unicode object is safe. Otherwise, the result is no longer safe.
        """
        t = super(SafeUnicode, self).__add__(rhs)
        if isinstance(rhs, SafeData):
            return SafeUnicode(t)
        return t

    def _proxy_method(self, *args, **kwargs):
        """
        Wrap a call to a normal unicode method up so that we return safe
        results. The method that is being wrapped is passed in the 'method'
        argument.
        """
        method = kwargs.pop('method')
        data = method(self, *args, **kwargs)
        if isinstance(data, str):
            return SafeString(data)
        else:
            return SafeUnicode(data)

    encode = curry(_proxy_method, method=unicode.encode)


def mark_safe(s):
    """
    Explicitly mark a string as safe for (HTML) output purposes. The returned
    object can be used everywhere a string or unicode object is appropriate.

    Can be called multiple times on a single string.
    """
    if isinstance(s, SafeData):
        return s
    if isinstance(s, str):
        return SafeString(s)
    if isinstance(s, unicode):
        return SafeUnicode(s)
    return SafeString(str(s))

