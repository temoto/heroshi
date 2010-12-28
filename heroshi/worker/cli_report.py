# coding: utf-8
"""Heroshi crawl reporter entry-point.

Reads \\n-separated list of JSON crawl results on stdin."""

import json
import logging, sys
from optparse import OptionParser

import heroshi, heroshi.api
from heroshi.log import update_loggers_level


def parse_params():
    usage_info = u"Usage: %prog [OPTION...]"
    version_info = u"Heroshi/" + heroshi.__version__
    opt_parser = OptionParser(usage_info, version=version_info)
    opt_parser.set_defaults(verbose=False, quiet=False, forever=False)
    opt_parser.add_option('-q', '--quiet', action="store_true",
                          help=u"Be quiet, don't generate any output")
    opt_parser.add_option('-v', '--verbose', action="store_true",
                          help=u"Be verbose, print detailed information")
    (options, args) = opt_parser.parse_args()
    return options, args

def main():
    options, args = parse_params()

    # set up logging
    if options.quiet:
        update_loggers_level(logging.CRITICAL)
    elif options.verbose:
        update_loggers_level(logging.DEBUG)

    for line in sys.stdin:
        line = line.strip()
        item = json.loads(line)
        item['result'] = item.pop('status')
        item.pop('cached', None)
        item.pop('success', None)
        heroshi.api.report_result(item)


if __name__ == '__main__':
    main()
