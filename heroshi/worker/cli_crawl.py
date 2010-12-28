"""Heroshi worker entry-point.

Reads \\n-separated list of URLs to crawl from stdin or command line args."""

import logging
from optparse import OptionParser

import heroshi
from heroshi.log import update_loggers_level
from heroshi.worker import Crawler


def parse_params():
    usage_info = u"Usage: %prog [OPTION...] [URL...]"
    version_info = u"Heroshi/%s" % heroshi.__version__
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, queue_size=500, connections=500)
    opt_parser.add_option('-q', '--quiet', action="store_true",
                          help=u"Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true",
                          help=u"Be verbose, print detailed information")
    opt_parser.add_option('-c', '--connections',
                          help=u"Maximum number of open connections. [Default = %default]", metavar="N")
    options, args = opt_parser.parse_args()
    return options, args

def main():
    options, args = parse_params()

    # set up logging
    if options.quiet:
        update_loggers_level(logging.CRITICAL)
    elif options.verbose:
        update_loggers_level(logging.DEBUG)

    crawler = Crawler(int(options.connections))
    for url in args:
        crawler.queue.put({'url': url, 'visited': None})

    try:
        crawler.crawl(forever=len(args) == 0)
    except KeyboardInterrupt:
        crawler.graceful_stop()


if __name__ == '__main__':
    main()
