

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
