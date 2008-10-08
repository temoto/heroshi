# -*- coding: utf-8 -*-

"""Heroshi queue protocol.
Gets URLs to crawl from queue server, crawls them, store pages
and send crawl info back to queue server.

GET : Data is number of items, client wishes to get
PUT : Data is pickled items, client wishes to send to server
QUIT : When worker receives this message, it stops any jobs and quits
"""

import sys, os, time
from optparse import OptionParser
import cPickle as pickle
import cjson as json
import random
from BeautifulSoup import BeautifulSoup
from twisted.protocols.basic import Int32StringReceiver
from twisted.internet.protocol import ServerFactory
from twisted.internet.protocol import ClientFactory
from twisted.internet import reactor
from twisted.internet.error import ConnectionDone
import urllib2
import urllib

import shared.misc
from shared.misc import HEROSHI_VERSION, debug
from shared.link import Link
from shared.page import Page
from shared.storage import save_page

BIND_PORT = 15822
KNOWN_ACTIONS = ( 'GET', 'PUT', 'QUIT', )


class ProtocolMessage(object):
    """Message within connection
    Could be action from client or response from server"""

    action = None
    status = None
    data = None

    def __init__(self, action=None, data=None, raw=None):
        """Creates new message.
        *data* is Python object for transfer. It will be serialized in .pack() method.
        *raw* is raw string from network. It will be unserialized and result will be available in .data"""

        self.action = action
        if data:
            self.data = data
        if raw:
            self.unpack(raw)

    def pack(self):
        packed = json.encode({'action': self.action, 'data': self.data})
        return packed

    def unpack(self, raw):
        unpacked = json.decode(raw)
        self.action = unpacked.get('action')
        self.status = unpacked.get('status')
        self.data = unpacked['data']

    def get_http_request_url(self, address, port):
        return "http://%s:%d/?r=%s" % (address, port, urllib.pathname2url(self.pack()))

    def __unicode__(self):
        return self.action or u'Empty protocol message'

    def __str__(self):
        return unicode(self)

    def __repr__(self):
        return unicode(self)
