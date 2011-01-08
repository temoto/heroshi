# coding: utf-8
"""Heroshi URL prefetcher entry-point. Run this to receive a pack of URLs to crawl now."""

import json
import logging, sys
from optparse import OptionParser

import heroshi, heroshi.api
from heroshi.log import update_loggers_level


FETCH_LIMIT = 1000


def parse_params():
    usage_info = u"Usage: %prog [OPTION...]"
    version_info = u"Heroshi/" + heroshi.__version__
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, forever=False)
    opt_parser.add_option('-q', '--quiet', action="store_true",
                          help=u"Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true",
                          help=u"Be verbose, print detailed information")
    opt_parser.add_option('-f', '--forever', action="store_true",
                          help=u"Repeat forever")
    opt_parser.add_option('-p', '--plain', action="store_true",
                          help=u"Print only URLs. If this option is unspecified, JSON objects will be printed.")
    (options, args) = opt_parser.parse_args()
    return options, args

def step(plain):
    items = heroshi.api.get_crawl_queue(FETCH_LIMIT)
    for item in items:
        assert item['url']
        if plain:
            sys.stdout.write(item['url'].encode('utf-8'))
        else:
            json.dump(item, sys.stdout)
        sys.stdout.write("\n")

def main():
    options, args = parse_params()

    # set up logging
    if options.quiet:
        update_loggers_level(logging.CRITICAL)
    elif options.verbose:
        update_loggers_level(logging.DEBUG)

    step(options.plain)

    if options.forever:
        while True:
            step(options.plain)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass # Silently stop, don't print traceback.
