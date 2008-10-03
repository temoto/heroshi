# -*- coding: utf-8 -*-

"""TODO"""

import sys, os, time
import cPickle
import urllib2
from BeautifulSoup import BeautifulSoup

import misc
from misc import HEROSHI_VERSION, debug
from link import Link
from form import Form
from storage import save_page


class Page(object):
    """TODO"""

    link = None
    html_content = ''
    lang = ''
    text_content = u''
    links = []
    forms = []
    visited = None

    def __init__(self, link, content):
        self.link = link
        self.html_content = content

    def parse(self):
        soup = BeautifulSoup(self.html_content)
        self.text_content = ''.join( [ e for e in soup.recursiveChildGenerator() if isinstance(e, unicode) ] )
        debug("parsed %d bytes of HTML into %d bytes of raw text" % (len(self.html_content), len(self.text_content)))
        self.find_links(soup)
        self.find_forms(soup)

    def find_links(self, soup=None):
        soup = soup or BeautifulSoup(self.html_content)
        a_tags = soup.findAll('a')
        for a in a_tags:
            href = a.get('href')
            if href:
                link = Link(href, self.link)
                self.links.append(link)

    def find_forms(self, soup=None):
        soup = soup or BeautifulSoup(self.html_content)
        form_tags = soup.findAll('form')
        for form_tag in form_tags:
            form = Form(self.link)
            form.init_attrs(**form_tag)
            # TODO: init_fields
            self.forms.append(form)

    def __unicode__(self):
        return "page at %s" % self.link.full

