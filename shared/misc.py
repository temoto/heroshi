# -*- coding: utf-8 -*-

HEROSHI_VERSION = '0.1'

params = None

# TODO: logger
def debug(msg):
    if not params.quiet:
        print(" * debug " + msg)

