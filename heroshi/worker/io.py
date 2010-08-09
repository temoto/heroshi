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
from heroshi.error import ApiError, CrawlError, FetchError, RobotsError


class IoWorkerDead(error.Error): pass


def new_state(is_closed):
    return is_closed, {}

class Worker(object):
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
            with_timeout(0.050, _read, self.worker, self.results,
                         timeout_value=None)

        self.worker.stdin.close()

    def fetch(self, url):
        """Fetches `url` using io-worker queue `q`. Blocks until `url`
        is fetched or crawler `is_closed`.

        Returns `None` when `is_closed` returns True.
        """
        log.debug("fetch %s", url)
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
        _write(self.worker, url)
        return out

def _io_op(op):
    try:
        return op()
    except IOError, e:
        if e.errno == errno.EPIPE:
            raise IoWorkerDead()
        else:
            raise

def _read(worker, results):
    result_str = _io_op(worker.stdout.readline)
    if not result_str:
        return False
    decoded = json.loads(result_str)
    for k in decoded:
        decoded[k.lower()] = decoded.pop(k)
    url = decoded['url']
    status = decoded.pop('status')
    decoded.pop('success')
    decoded['result'] = u"OK" if status == u"200 OK" else u"non-200: " + status
    decoded['status_code'] = decoded.pop('statuscode')
    decoded['content'] = decoded.pop('body')

    log.debug("  got: %s", url)
    if url not in results:
        log.error("Got result for non-requested url: %s.", url)
        return True

    out = results.pop(url)
    out.send(decoded)
    return True

def _write(worker, url):
    _io_op(lambda: worker.stdin.write(url + '\n'))
    return True
