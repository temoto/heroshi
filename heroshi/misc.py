"""Random utility functions."""

from eventlet import greenlet, greenthread
import os
import pytz
import sys


def to_utc(dt, timezone):
    tz = pytz.timezone(timezone)
    return tz.localize(dt, is_dst=True).astimezone(pytz.UTC)

# From http://www.xhaus.com/alan/python/httpcomp.html#gzip
# Used without permission.
def gzip_string(s, level=6):
    """Compress string using gzip.
    Default compression level is 6"""

    from cStringIO import StringIO
    from gzip import GzipFile
    zbuf = StringIO()
    zfile = GzipFile(mode='wb', compresslevel=level, fileobj=zbuf)
    zfile.write(s)
    zfile.close()
    return zbuf.getvalue()

def os_path_expand(p):
    return os.path.expandvars(os.path.expanduser(p))

def reraise_errors(t, master_gt):
    try:
        return t.wait()
    except greenlet.GreenletExit:
        pass
    except Exception:
        greenthread.kill(master_gt, *sys.exc_info())

