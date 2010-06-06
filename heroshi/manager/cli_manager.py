"""Heroshi URL server entry-point. Run this to start serving URLs for workers."""

import eventlet, eventlet.wsgi
import logging
from optparse import OptionParser

import heroshi
from heroshi.conf import settings
from heroshi.log import update_loggers_level
from .server import manager_pool, wsgi_app


class Blackhole(object):
    """File-like object which just discards all writes."""

    def write(self, data):
        pass


def parse_params():
    usage_info = u"Usage: %prog [OPTION...] [URL...]"
    version_info = u"Heroshi/%s" % heroshi.__version__
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, queue_size=500, connections=500)
    opt_parser.add_option('-q', '--quiet', action="store_true",
                          help=u"Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true",
                          help=u"Be verbose, print detailed information")
    options, args = opt_parser.parse_args()
    return options, args

def main():
    options, _args = parse_params()

    # set up logging
    if options.quiet:
        update_loggers_level(logging.CRITICAL)
    elif options.verbose:
        update_loggers_level(logging.DEBUG)
    else:
        update_loggers_level(settings.log['level'])

    # eager initialization of manager instance and its storage connection
    with manager_pool.item() as manager:
        manager.active = True

    sock = eventlet.listen( ('0.0.0.0', 8080) )
    eventlet.wsgi.server(sock, wsgi_app, log=Blackhole())


if __name__ == '__main__':
    main()
