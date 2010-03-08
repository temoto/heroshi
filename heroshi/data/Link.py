import re, hashlib


re_url_full = re.compile(r'^(https?)://.*')
re_url_domain = re.compile(r'^(https?)://([^/]+)(/?.*)')
re_base_domain = re.compile(r'(ww[^\.]*\.)?(.+)')

HASHER = hashlib.sha1


class Link(object):
    protocol = property(lambda self: self.is_secure and 'https' or 'http')
    full = property(lambda self: self.url if self.is_full else '%s://%s%s' % (self.protocol, self.domain, self.url))
    relative = property(lambda self: self.url if not self.is_full else 1/0) # TODO: implement full url shortener

    def __init__(self, url, parent_link=None):
        self.domain = ''
        self.base_domain = ''
        self.is_subdomain = False
        self.is_external = False

        self.url = url.lower() # lowercase is important. url regexps are case-sensitive
        self.is_secure = self.url.startswith('https')
        self.is_full = re_url_full.search(self.url) is not None
        if self.is_full:
            m = re_url_domain.search(self.url)
            if m:
                self.domain = m.group(2)
                self.base_domain = re_base_domain.search(self.domain).group(2)
        if parent_link:
            if not self.is_full:
                self.is_secure = parent_link.is_secure
                self.domain = parent_link.domain
                self.base_domain = parent_link.base_domain
            self.is_external = parent_link.base_domain != self.base_domain
            if self.is_external:
                self.is_subdomain = parent_link.base_domain in self.base_domain
        else:
            self.is_external = self.is_full
        if not self.is_full:
            if not parent_link:
                raise ValueError("Relative URL is useless without parent")
            if not self.url.startswith('/'):
                self.url = '/' + self.url

    def hash(self):
        if not self.domain: # whether self.full is even possible
            raise ValueError("Getting hash of relative URL is useless.")
        hasher = HASHER(self.full)
        return hasher.hexdigest()

    def __unicode__(self):
        flags = u''
        flags += 'F' if self.is_full else 'r'
        flags += 'x' if self.is_subdomain else 'X' if self.is_external else 'd'
        flags += 'S' if self.is_secure else 'u'
        s = u'%s [%s]' % (self.url, flags)
        if not self.is_full:
            s += u' @ %s' % self.domain
        return s

    def __str__(self): return str(unicode(self))
    def __repr__(self): return unicode(self)
