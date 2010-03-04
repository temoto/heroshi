# coding: utf-8
from eventlet import spawn_after
from eventlet.pools import Pool


class FactoryPool(Pool):
    def __init__(self, constructor_pack, min_size=0, max_size=4, order_as_stack=False):
        self.constructor_pack = constructor_pack
        super(FactoryPool, self).__init__(min_size, max_size, order_as_stack)

    def create(self):
        fun, args, kwargs = self.constructor_pack
        return fun(*args, **kwargs)


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
            pool = self._pools[key] = FactoryPool( (self.func, args, kwargs),
                                                  max_size=self.pool_max_size)
        if self.timeout is not None:
            self.stop_timer(key)

        return pool.get()

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
