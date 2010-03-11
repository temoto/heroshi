# coding: utf-8
from eventlet import spawn_after


class Cache(dict):
    def __init__(self, *args, **kwargs):
        super(Cache, self).__init__(*args, **kwargs)
        self._timers = {}

    def __delitem__(self, key):
        self.stop_timer(key)
        super(Cache, self).__delitem__(key)

    def clear(self):
        for timer in self._timers.values():
            timer.cancel()
        self._timers.clear()
        super(Cache, self).clear()

    def set(self, key, value, timeout=None):
        self[key] = value

        if timeout is not None:
            self.reset_timer(key, timeout)

    def stop_timer(self, key):
        if key in self._timers:
            timer = self._timers.pop(key)
            timer.cancel()

    def reset_timer(self, key, timeout):
        self.stop_timer(key)
        self._timers[key] = spawn_after(timeout, self.__delitem__, key)

    def __unicode__(self):
        return u"<Cache of %d items>" % (len(self),)

    def __str__(self): return str(unicode(self))
    def __repr__(self): return unicode(self)

