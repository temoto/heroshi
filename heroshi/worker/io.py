# coding: utf-8
"""Heroshi worker: IO-worker interaction.
"""
import errno
from eventlet import sleep, with_timeout
from eventlet.queue import Event
import json
import subprocess

from heroshi import error, get_logger
log = get_logger("worker.io")


class IoWorkerDead(error.Error): pass


class Worker(object):
    """IO worker.
    """

    def __init__(self, is_closed):
        self.is_closed = is_closed
        self.results = {}
        self.worker = None

    def run_loop(self):
        """Runs io-worker until it dies.

        You SHOULD spawn this function (so it runs in separate thread).
        """
        args = ["io-worker/io-worker", "skip-robots"]
        self.worker = subprocess.Popen(args, bufsize=1,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE)

        while not self.is_closed():
            encoded = with_timeout(0.050, _io_op, self.worker.stdout.readline,
                                   timeout_value="")
            encoded = encoded.strip()
            if not encoded:
                continue

            url, result = _parse(encoded)
            if url is None:
                continue

            if url in self.results:
                out = self.results.pop(url)
                out.send(result)
            else:
                log.error("Got result for non-requested url: %s.", url)

        self.worker.stdin.close()

    def fetch(self, url):
        """Fetches `url` using io-worker queue `q`. Blocks until `url`
        is fetched or crawler `is_closed`.

        Returns `None` when `is_closed` returns True.
        """
        #log.debug("fetch %s", url)
        result = self.start_fetch(url)
        # This sleep loop provides a way to stop on crawler close.
        while not result.ready():
            if self.is_closed():
                return None
            sleep(0.010)
        return result.wait()

    def start_fetch(self, url):
        """Almost-non-blocking version of `fetch`. Returns an `Event` on which
        you can `.wait()` for fetch result.

        Actually it may block attempting to write a line to io-worker
        process, but this is a feature.
        """
        out = self.results.get(url)
        if out is not None:
            log.debug("Reusing fetch-in-progress for %s.", url)
            return out
        else:
            self.results[url] = out = Event()
        _io_op(lambda: self.worker.stdin.write(url + '\n'))
        return out

def _io_op(op):
    """Wrapper for IO operations. Runs `op` and returns its result.
    Raises `IoWorkerDead` on EPIPE error.
    """
    try:
        return op()
    except IOError, e:
        if e.errno == errno.EPIPE:
            raise IoWorkerDead()
        else:
            raise

def _parse(encoded):
    """Parses encoded io-worker result. Returns (url, result) tuple on success
    or (None, error).
    """
    try:
        decoded = json.loads(encoded)
    except ValueError, e:
        try:
            debug_info = json.dumps({'io-worker-data': encoded})
        except UnicodeDecodeError:
            debug_info = u"And can't encode debug info."
        log.error(u"Can't decode incoming data: %s", debug_info)
        return None, e

    url = decoded['url']
    decoded['result'] = decoded.pop('status')

    return url, decoded
