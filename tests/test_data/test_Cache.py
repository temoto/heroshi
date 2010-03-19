import eventlet
import smock
import unittest

from heroshi.data import Cache


# Very unprobably, but still may be you need to raise this value on slow machine if most tests fail.
DEFAULT_TIMEOUT = 0.002


class ExpireTestCase(unittest.TestCase):
    def setUp(self):
        self.cache = Cache()

    def test_timer_001(self):
        """Must delete items after timeout."""
        self.cache.set('key-timer_001', "value", DEFAULT_TIMEOUT / 2)
        eventlet.sleep(DEFAULT_TIMEOUT)
        self.assertFalse('key-timer_001' in self.cache)

    def test_race_001(self):
        """Expiry must not conflict with regular `del`."""
        self.cache.set('key-delete_race_001', "value", DEFAULT_TIMEOUT / 2)
        del self.cache['key-delete_race_001']
        eventlet.sleep(DEFAULT_TIMEOUT)
        self.assertFalse('key-delete_race_001' in self.cache)

    def test_race_002(self):
        """Expiry must not conflict with regular `pop`."""
        self.cache.set('key-delete_race_002', "value", DEFAULT_TIMEOUT / 2)
        self.cache.pop('key-delete_race_002')
        eventlet.sleep(DEFAULT_TIMEOUT)
        self.assertFalse('key-delete_race_002' in self.cache)

    def test_race_003(self):
        """Expiry must not conflict with regular `del` in another thread."""
        self.cache.set('key-delete_race_003', "value", DEFAULT_TIMEOUT / 2)
        eventlet.spawn_n(self.cache.__delitem__, 'key-delete_race_003')
        eventlet.sleep(DEFAULT_TIMEOUT)
        self.assertFalse('key-delete_race_003' in self.cache)

    def test_race_004(self):
        """Expiry must not conflict with regular `pop` in another thread."""
        self.cache.set('key-delete_race_004', "value", DEFAULT_TIMEOUT / 2)
        eventlet.spawn_n(self.cache.pop, 'key-delete_race_004')
        eventlet.sleep(DEFAULT_TIMEOUT)
        self.assertFalse('key-delete_race_004' in self.cache)


class InterfaceTestCase(unittest.TestCase):
    def test_dict_constructor_001(self):
        """`Cache` constructor must behave like dict's one."""
        d = {}
        c = Cache(d)
        self.assertEqual(c.items(), [])

    def test_dict_constructor_002(self):
        """`Cache` constructor must behave like dict's one."""
        d = {'foo': "bar"}
        c = Cache(d)
        self.assertEqual(c['foo'], "bar")

    def test_set_001(self):
        c = Cache()
        c.set('key-set_001', "value")
        self.assertEqual(c.get('key-set_001'), "value")

