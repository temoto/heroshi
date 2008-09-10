# -*- coding: utf-8 -*-

"""Crawler worker.
Gets URLs to crawl from queue server, crawls them, store and send crawl info back to queue server"""

import sys, os, time
from optparse import OptionParser
import cPickle
import random
from BeautifulSoup import BeautifulSoup
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet import reactor
from twisted.internet.error import ConnectionDone
from twisted.internet import reactor

import misc
from misc import HEROSHI_VERSION, debug
from link import Link
from page import Page
from storage import save_page

BIND_PORT = 15822


class ProtocolAction(object):
    """One simple action within connection with server, i.e.: GET, PUT"""

    action = None
    data = None

    on_done = None

    def __init__(self, action, data, on_done=None):
        """Creates new action.
        Data is ready-to-network sending string"""
        self.action = action
        self.data = data
        self.on_done = on_done

    def pack(self):
        return "%s.%s" % (self.action, self.data)

    def done(self):
        # no handler is set
        if not self.on_done:
            return
        try:
            for handler in self.on_done:
                handler(self)
        except TypeError:
            pass
        try:
            self.on_done(self)
        except TypeError:
            pass

    def __unicode__(self):
        return self.action

    def __str__(self):
        return unicode(self)

    def __repr__(self):
        return unicode(self)


class ActionGet(ProtocolAction):
    """GET action
    Data is number of items, client wishes to get"""

    num = 0

    def __init__(self, num, on_done=None):
        self.action = 'GET'
        try:
            self.num = int(num)
        except:
            raise Exception, "num should be integer"
        super(ActionGet, self).__init__('GET', str(self.num), on_done)

class ActionPut(ProtocolAction):
    """PUT action
    Data is pickled items, client wishes to send to server"""

    items = []

    def __init__(self, items, on_done=None):
        self.action = 'PUT'
        self.items = items
        pickled = cPickle.dumps(self.items)
        super(ActionPut, self).__init__('PUT', pickled, on_done)


class ActionClose(ProtocolAction):
    """CLOSE action
    For this action, worker stops any jobs and quits."""

    def __init__(self, on_done=None):
        self.action = 'CLOSE'


def read_action(s):
    """Parses string from network and returns appropriate ActionXxx"""

    action_id, data = s.split('.', 1)
    action_classes_map = {
        'GET': ActionGet,
        'PUT': ActionPut,
    }
    if action_id not in action_classes_map:
        # TODO: custom exception
        raise Exception, "Incorrect protocol used. Action %s is not recognized" % action_id
    action_class = action_classes_map[action_id]
    try:
        action = action_class(action_id, data)
    except:
        # TODO: custom exception
        raise Exception, "Failed to create action instance of %s" % action_class
    return action

class QueueManagement(Int32StringReceiver):
    """Twisted Protocol"""

    current_action = None
    on_disconnect = None

    def connectionMade(self):
#         debug("connected to server")
        self.current_action = self.factory.current_action
        self.do()

    def do(self):
        if self.current_action is None:
            debug("nothing else to do")
            self.transport.loseConnection()
        elif type(self.current_action) is ActionGet:
            debug("requesting %d items" % self.current_action.num)
            self.sendString(self.current_action.pack())
        elif type(self.current_action) is ActionPut:
            debug("sending %d items" % len(self.current_action.items))
            self.sendString(self.current_action.pack())
        elif type(self.current_action) is ActionClose:
            debug("got close action, exiting on disconnect")
            self.transport.loseConnection()
        else:
            debug("unknown factory.action %s. Panic" % self.current_action)

    def connectionLost(self, reason):
        if self.on_disconnect:
            self.on_disconnect(self, reason)
        if reason.check(ConnectionDone):
            debug("disconnected")
        else:
            debug("connection with server lost: %s" % reason)

    def stringReceived(self, string):
#         debug("recieved: %s" % string)
        try:
            status, data = string.split('.', 1)
            if status == 'EMPTY':
                self.action_empty()
            elif status == 'TAKE':
                items = cPickle.loads(data)
                self.action_take(items)
            elif status == 'DELETE':
                items = cPickle.loads(data)
                self.action_delete(items)
            elif status == 'OK':
                self.action_ok()
        except ValueError: # and UnpicklingError
            debug("incorrect protocol used. Disconnecting")
            self.transport.loseConnection()

    def action_empty(self):
        debug("server is out of items")
        if type(self.current_action) != ActionGet:
            debug("unexpected EMPTY. disconnecting from broken server")
        self.next_action()

    def action_take(self, items):
        debug("server offered %d items" % len(items))
        self.factory.items += items
        self.next_action()

    def action_delete(self, items):
        debug("server said to delete %d items" % len(items))
        self.factory.items -= items

    def action_ok(self):
        debug("server said `ok` who-ho, moving to next action")
        self.next_action()

    def next_action(self):
        old_action = self.factory.current_action
        if old_action:
            old_action.done()
        new_action = self.factory.next_action()
        if new_action is not None:
            debug("action %s done. doing: %s" % (old_action, new_action))
            self.current_action = new_action
            self.do()
        else:
            debug("all actions done. disconnecting.")
            self.transport.loseConnection()


class QueueClientFactory(ClientFactory):
    """Twisted client Factory"""

    current_action = None
    actions = []
    items = []
    num = 0
    on_disconnect = None

    def __init__(self, actions, items=None, num=0, on_disconnect=None):
        debug("created client queue factory with %d items, actions: %s" % (len(items) if items else 0, actions))
        self.protocol = QueueManagement
        self.actions = actions
        self.actions.reverse()
        self.num = num
        if items:
            self.items = items
        self.protocol.on_disconnect = self.on_protocol_disconnect
        self.next_action()

    def next_action(self):
        if self.current_action:
            self.current_action.done()
        if len(self.actions):
            self.current_action = self.actions.pop()
            return self.current_action
        else:
            return

    def on_protocol_disconnect(self, protocol, reason):
        debug("factory disconnected")
        if type(self.current_action) is ActionClose:
            reactor.stop()
            self.current_action.done()
        if self.on_disconnect:
            self.on_disconnect(protocol, reason)


