# -*- coding: utf-8 -*-
"""Workers spawner
Keeps N workers running. That's all it does, really."""

import os, sys, time
import subprocess

import misc
from misc import debug

WORKER_EXECV = ['python', 'crawler.py']

workers = []

def start_worker():
    proc = subprocess.Popen(WORKER_EXECV)
    workers.append(proc)

def on_SIGCHLD(child):
    # TODO: handle worker termination
    pass

def run(num):
    for i in xrange(num):
        start_worker()
    while True:
        time.sleep(1)


class WorkerManagerParameters(object):
    """Command-line parameters"""
    silent = False
    # TODO: verbose, queue location

    def __init__(self, argv):
        assert(len(argv))
        # FIXME: use getopt
        if "-q" in argv or "--quiet" in argv or "--silent" in argv:
            self.silent = True


def main():
    misc.params = WorkerManagerParameters(sys.argv)
    num = int(sys.argv[1])
    debug("Spawning %d workers..." % num)
    run(num)

if __name__ == '__main__':
    main()

