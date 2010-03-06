import logging
from optparse import OptionParser

from shared import HEROSHI_VERSION
from shared.misc import init_logging
from shared.conf import settings
from .worker import Crawler


def parse_params():
    usage_info = "Usage: %prog [OPTION...]"
    version_info = "Heroshi/%s" % HEROSHI_VERSION
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, queue_size=500, connections=500)
    opt_parser.add_option('-q', '--quiet', action="store_true", help="Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true", help="Be verbose, print detailed information")
    opt_parser.add_option('-Q', '--queue-size', help="Maximum queue size. Crawler will ask queue server for this many of items. [Default = 2000]", metavar="N")
    opt_parser.add_option('-c', '--connections', help="Maximum number of open connections. [Default = 250]", metavar="N")
    (options, args) = opt_parser.parse_args()
    return options, args

def main():
    (options, _args) = parse_params()

    # set up logging
    log_level = settings.get('loglevel')
    if options.quiet:
        log_level = logging.CRITICAL
    elif options.verbose:
        log_level = logging.DEBUG
    init_logging(level=log_level)

    crawler = Crawler(int(options.queue_size), int(options.connections))
    try:
        crawler.crawl()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
