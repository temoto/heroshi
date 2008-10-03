#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from manager.tests import CrawlQueueTestCase, ManagerTestCase
from worker.tests import WorkerTestCase
from protocol.tests import QueueServerProtocolTestCase
from shared.call_indicator import CallIndicatorTestCase


if __name__ == '__main__':
    unittest.main()
