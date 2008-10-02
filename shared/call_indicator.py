# -*- coding: utf-8 -*-

"""Autowrapping functions intended for testing purposes.
Deferred checking whether some wrapped function was called."""

# TODO: lock history in clean_calls()

import threading
import unittest


_history = []
_saved = {}


def register_call(full_name, args=None, kwargs=None):
    global _history

    _history.append( (full_name, args or [], kwargs or {}) )

def get_call(full_name):
    global _history

    for call_item in _history:
        if call_item[0] == full_name:
            return call_item
    else:
        return None

def is_called(full_name):
    return get_call(full_name) is not None

def get_full_name(parent, func_name):
    if getattr(parent, '__name__', None) is not None:
        # it is module, or module-like. we're happy with __name__
        func_parent = parent
    elif getattr(parent, '__class__', None) is not None:
        # this is not module, maybe it is a class instance?
        func_parent = parent.__class__
    else:
        raise Exception, "Unknown thing: %s. Should be either module or class instance." % parent
    return "%s.%s" % (func_parent.__name__, func_name)

def clean_calls(name):
    global _history

    # TODO: lock history
    new_history = filter(lambda call: call[0] != name, _history)
    _history = new_history

def install(module, func_name, replacement_func):
    global _saved

    def indicator_wrapper(full_name, *args, **kwargs):
        register_call(full_name, args, kwargs)
        return replacement_func(*args, **kwargs)

    full_name = get_full_name(module, func_name)
    if full_name in _saved:
        raise Exception, "Wrapper is already installed for %s" % full_name
    _saved[full_name] = (module, func_name, getattr(module, func_name))
    setattr(module, func_name,
        lambda *args, **kwargs: indicator_wrapper(full_name, *args, **kwargs))

def install_simple(module, func_name):
    install(module, func_name, lambda *args, **kwargs: None)

def restore(full_name, no_del=False):
    global _saved

    if full_name not in _saved:
        raise Exception, "No indicator is installed for %s" % full_name
    module, func_name, saved_func = _saved[full_name]
    setattr(module, func_name, saved_func)
    if not no_del:
        del _saved[full_name]

def restore_all():
    global _saved

    # TODO: lock saved
    for full_name, save in _saved.iteritems():
        restore(full_name, no_del=True)
    _saved.clear()

def clean_all():
    global _history

    del _history[:]


class CallIndicatorTestCase(unittest.TestCase):
    class TestParent(object):
        """Simulates module if you want"""

        def __init__(self, be_module=False):
            if be_module:
                self.__name__ = self.__class__.__name__

        def func1(self, x):
            return x + 1

        def func2(self, x):
            return x -1

    def setUp(self):
        self.parent = CallIndicatorTestCase.TestParent()

    def tearDown(self):

        global _history, _saved

        _history = []
        _saved = {}

    def test_register_call_001(self):
        """must register call"""

        self.assertEqual(self.parent.func1(1), 2)
        install_simple(self.parent, 'func1')
        self.parent.func1(1)
        self.assertTrue(is_called('TestParent.func1'))

    # TODO: test register_call twice -> same result

    def test_get_call_001(self):
        pass

    def test_is_called_001(self):
        pass

    def test_get_full_name_001(self):
        # TODO: write it now
        pass

    def test_clean_calls_001(self):
        pass

    def test_install_001(self):
        """must install custom indicator"""

        self.assertEqual(self.parent.func1(1), 2)
        install(self.parent, 'func1', lambda x: x + 2)
        self.assertEqual(self.parent.func1(1), 3)

    def test_install_002(self):
        """must save original function"""

        global _saved

        install(self.parent, 'func1', lambda x: x + 2)
        self.assertTrue('TestParent.func1' in _saved)

    # TODO: test install twice -> exception

    def test_install_simple_001(self):
        """must install simple indicator"""

        install_simple(self.parent, 'func1')
        self.assertEqual(self.parent.func1(1), None)
        self.assertTrue(is_called('TestParent.func1'))

    def test_restore_001(self):
        """must restore function to original"""

        install_simple(self.parent, 'func1')
        restore('TestParent.func1')
        self.assertEqual(self.parent.func1(1), 2)

    def test_restore_002(self):
        """must delete original function from save-list"""

        global _saved

        install_simple(self.parent, 'func1')
        self.assertTrue('TestParent.func1' in _saved)
        restore('TestParent.func1')
        self.assertFalse('TestParent.func1' in _saved)

    # TODO: test restore twice -> exception

    def test_restore_all_001(self):
        """must restore all original functions"""

        install_simple(self.parent, 'func1')
        install_simple(self.parent, 'func2')
        restore_all()
        self.assertEqual(self.parent.func1(1), 2)
        self.assertEqual(self.parent.func2(1), 0)

    def test_clean_all_001(self):
        """must clean all calls"""

        install_simple(self.parent, 'func1')
        install_simple(self.parent, 'func2')
        self.parent.func1(1)
        self.parent.func1(2)
        self.parent.func2(1)
        self.parent.func2(2)
        clean_all()
        self.assertFalse(is_called('TestParent.func1'))
        self.assertFalse(is_called('TestParent.func2'))


if __name__ == '__main__':
    unittest.main()
