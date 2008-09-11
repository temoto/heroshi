# -*- coding: utf-8 -*-

import os, sys, time
import random
import cPickle
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ServerFactory
from twisted.internet import reactor
from twisted.internet.error import ConnectionDone

import client_protocol
import misc
from misc import debug

os_path_expand = lambda p: os.path.expandvars(os.path.expanduser(p))

QUEUE_PATH = os_path_expand('~/.heroshi/queue')
MANAGER_SOCKET = os_path_expand('~/.heroshi/manager.sock')
WORKER_BUFFER = 10

class CrawlManagement(Int32StringReceiver):
    """Twisted Protocol"""

    def connectionMade(self):
        assert(hasattr(self.factory, 'crawl_queue'))
        debug("peer connected")

    def connectionLost(self, reason):
        if reason.check(ConnectionDone):
            debug("peer disconnected")
        else:
            debug("connection with peer lost: %s" % reason)

    def stringReceived(self, string):
        debug("recieved: %s" % string)
        try:
            action = client_protocol.read_action(string)
            if type(action) is client_protocol.ActionGet:
                self.action_get(action.num)
            elif type(action) is client_protocol.ActionPut:
                self.action_put(action.items)
        except ValueError: # and UnpicklingError
            debug("incorrect protocol used. Kicking peer")
            self.transport.loseConnection()

    def action_get(self, num):
        debug("peer requested %d items, crawl queue has %d items" % (num, len(self.factory.crawl_queue)))
        send_num = min(num, len(self.factory.crawl_queue))
        debug("giving %d items" % send_num)
        if send_num >= 1 and len(self.factory.crawl_queue):
            items = random.sample(self.factory.crawl_queue, send_num)
            pickled = cPickle.dumps(items)
            self.sendString('TAKE.' + pickled)
        else:
            self.sendString('EMPTY.')

    def action_put(self, items):
        debug("peer offered %d items" % len(items))
        # TODO: better merge
        for qi in self.factory.crawl_queue:
            if qi in items:
                items.remove(qi)
        self.factory.crawl_queue += items
        self.sendString('OK.')


class CrawlManagementFactory(ServerFactory):
    """Twisted Factory"""

    protocol = CrawlManagement
    crawl_queue = None

    def __init__(self, crawl_queue):
        self.crawl_queue = crawl_queue


class CrawlQueue(object):
    """Persistent list"""

    queue = []
    store_path = None

    def __init__(self, store_path):
        self.store_path = store_path
        self.load_queue()

    def load_queue(self):
        if not os.path.exists(self.store_path):
            return
        f = open(self.store_path, 'rb')
        self.queue = cPickle.load(f)
        f.close()

    def save_queue(self):
        path_dir = os.path.dirname(self.store_path)
        if not os.path.isdir(path_dir):
            os.makedirs(path_dir)
        f = open(self.store_path, 'wb')
        cPickle.dump(self.queue, f)
        f.close()

    def __iter__(self):
        return iter(self.queue)

    def __getitem__(self, index):
        return self.queue.__getitem__(index)

    def __len__(self):
        return len(self.queue)

    def __add__(self, what):
        return self.queue.__add__(what)


class QueueManagerParameters(object):
    """Command-line parameters"""
    quiet = False
    # TODO: verbose, queue location

    def __init__(self, argv):
        assert(len(argv))
        # FIXME: use getopt
        if "-q" in argv or "--quiet" in argv or "--silent" in argv:
            self.quiet = True


def main():
    misc.params = QueueManagerParameters(sys.argv)
    crawl_queue = CrawlQueue(QUEUE_PATH)
    try:
        crawl_factory = CrawlManagementFactory(crawl_queue)

        reactor.listenTCP(client_protocol.BIND_PORT, crawl_factory, interface="127.0.0.1")
        debug("Ready. Loaded queue of %d items. Accepting connections..." % len(crawl_factory.crawl_queue))
        reactor.run()
        crawl_queue.queue = crawl_factory.crawl_queue
    finally:
        debug("Gracefully shutting down")
        crawl_queue.save_queue()

if __name__ == '__main__':
    main()

