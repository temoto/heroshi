from eventlet import tpool
import random
import socket

from heroshi import error, misc
log = misc.get_logger("dns")
from heroshi.data import Cache


DEFAULT_TTL = 1200
NX_TTL = 3600


class DnsError(error.Error):
    """Base class for DNS errors."""
    pass


class NXDomainError(DnsError):
    """Domain does not exist."""
    pass


class NoAddressError(DnsError):
    """Domain has no A record."""
    pass


class CachingResolver(object):
    def __init__(self):
        self.cache = Cache()

    def resolve(self, hostname):
        """hostname -> ([addr], ttl)"""
        try:
            _primary_hostname, _aliases, addrs = tpool.execute(socket.gethostbyname_ex, hostname)
        except TypeError, e:
            raise DnsError(unicode(e))
        except socket.gaierror, (err, desc):
            # This is actually a bug in gethostbyname_ex: it returns EAI_NODATA for both cases
            # nxdomain and noerror with zero A records.
            # With better resolver, these errors must be distinguished, because 'zero records' response
            # may still supply a TTL, which is unknown in case of nxdomain error.
            if err == socket.EAI_NODATA:
                return NoAddressError(unicode(desc+": "+hostname)), NX_TTL
            else:
                raise DnsError(unicode(desc+": "+hostname))
        else:
            return addrs, DEFAULT_TTL
        assert False, u"CachingResolver.resolve() supposed to return somewhere earlier."

    def gethostbyname(self, hostname):
        """Idempotent hostname -> addr."""
        try:
            addrs = self.cache[hostname]
        except KeyError:
            addrs, ttl = self.resolve(hostname)
            if addrs == [hostname]:
                # hostname was valid address in first place. Don't clutter cache with it.
                return hostname
            self.cache.set(hostname, addrs, ttl)
        if isinstance(addrs, (NXDomainError, NoAddressError)):
            raise addrs
        elif len(addrs) == 0:
            raise NoAddressError(u"Domain has no address: "+hostname)
        else:
            return random.choice(addrs)
