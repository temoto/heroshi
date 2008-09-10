# -*- coding: utf-8 -*-

HEROSHI_VERSION = '0.1'

params = None

def debug(msg):
    # TODO: syslog ?
    if not params.quiet:
        print(" * debug " + msg)

