import logging
from optparse import OptionParser

import heroshi
from heroshi.misc import update_loggers_level
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
    opt_parser.add_option('-Q', '--queue-size',
                          help=u"Maximum queue size. Crawler will try to keep its queue filled up to this value. [Default = %default]",
                          metavar="N")
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

    if len(args) > 0:
        max_queue_size = len(args)
    else:
        max_queue_size = int(options.queue_size)

    crawler = Crawler(max_queue_size, int(options.connections))
    for url in args:
        crawler.queue.put({'url': url, 'visited': None})

    try:
        crawler.crawl(forever=len(args) == 0)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
