"""Map of pools data structure implementation module. See `PoolMap` class for more info."""

from contextlib import contextmanager
from eventlet.pools import Pool

from .Cache import Cache


class PoolMap(object):
    """TODO"""

    def __init__(self, func, timeout=None, pool_max_size=1):
        self.func = func
        self.timeout = timeout
        self.pool_max_size = pool_max_size
        self._pools = Cache()

    def get(self, key, *args, **kwargs):
        try:
            pool = self._pools[key]
            self._pools.stop_timer(key)
        except KeyError:
            pool = self._pools[key] = Pool(max_size=self.pool_max_size)
            pool.create = lambda: self.func(*args, **kwargs)

        return pool.get()

    @contextmanager
    def getc(self, key, *args, **kwargs):
        item = self.get(key, *args, **kwargs)
        yield item
        self.put(key, item)

    def put(self, key, value):
        pool = self._pools[key]
        pool.put(value)

        # this must be done after `pool.put(value)`, because pool's free counter gets updated then
        if self.timeout is not None:
            if pool.free() >= pool.max_size:
                self._pools.reset_timer(key, self.timeout)

    def __unicode__(self):
        return u"<PoolMap of %d pools>" % (len(self._pools),)

    def __str__(self): return str(unicode(self))
    def __repr__(self): return unicode(self)
