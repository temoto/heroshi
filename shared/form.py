# -*- coding: utf-8 -*-

import re, hashlib

from link import Link

class Form(object):
    action = None # Link
    fields = []

    def __init__(self, parent_link):
        pass

    def init_attrs(**attrs):
        pass

    def init_fields(**fields):
        pass

    def __unicode__(self):
        return u'form'

