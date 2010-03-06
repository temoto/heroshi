# coding: utf-8
"""TODO"""

from BeautifulSoup import BeautifulSoup

from heroshi.data import Link
from heroshi.misc import get_logger
log = get_logger()


class Page(object):
    """TODO"""

    def __init__(self, link, content):
        self.link = link
        self.html_content = content
        self.lang = ''
        self.text_content = u''
        self.links = []
        self.forms = []
        self.visited = None

    def parse(self):
        soup = BeautifulSoup(self.html_content)
        self.text_content = u''.join( e for e in soup.recursiveChildGenerator() if isinstance(e, unicode) )
#        log.debug("parsed %d bytes of HTML into %d bytes of raw text",
#                len(self.html_content), len(self.text_content))
        self.find_links(soup)
#         self.find_forms(soup)

    def find_links(self, soup=None):
        soup = soup or BeautifulSoup(self.html_content)
        a_tags = soup.findAll('a')
        for a in a_tags:
            href = a.get('href')
            if href:
                link = Link(href, self.link)
                self.links.append(link)

#     def find_forms(self, soup=None):
#         soup = soup or BeautifulSoup(self.html_content)
#         form_tags = soup.findAll('form')
#         for form_tag in form_tags:
#             form = Form(self.link)
#             form.init_attrs(**form_tag)
#             # TODO: init_fields
#             self.forms.append(form)

    def __unicode__(self):
        return u"<Page at %s>" % self.link.full

    def __str__(self): return str(unicode(self))
    def __repr__(self): return unicode(self)
