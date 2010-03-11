# coding: utf-8
from contextlib import contextmanager
from eventlet import spawn_after
from eventlet.pools import Pool


class PoolMap(object):
    def __init__(self, func, timeout=None, pool_max_size=1):
        self.func = func
        self.timeout = timeout
        self.pool_max_size = pool_max_size
        self._pools = {}
        self._timers = {}

    def get(self, key, *args, **kwargs):
        if key in self._pools:
            pool = self._pools[key]
        else:
            pool = Pool(max_size=self.pool_max_size)
            pool.create = lambda: self.func(*args, **kwargs)
            self._pools[key] = pool
        if self.timeout is not None:
            self.stop_timer(key)

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
                self.reset_timer(key)

    def stop_timer(self, key):
        if key in self._timers:
            timer = self._timers.pop(key)
            timer.cancel()

    def reset_timer(self, key, timeout=None):
        self.stop_timer(key)

        def fun():
            self._pools.pop(key)
            self._timers.pop(key)

        self._timers[key] = spawn_after(timeout or self.timeout, fun)

    def __unicode__(self):
        return u"<PoolMap of %d pools>" % (len(self._pools),)

    def __str__(self): return str(unicode(self))
    def __repr__(self): return unicode(self)
