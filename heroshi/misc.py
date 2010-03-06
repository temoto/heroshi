import os, logging
from logging.handlers import SysLogHandler
from logging import StreamHandler

from heroshi.conf import settings
from heroshi.error import MissingOption


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


# Handlers cache is to open a single fd to write to syslog and stderr.
_log_handlers = {}
# Loggers cache allows to update all existing loggers' level
# with a single call of the function `update_loggers_level` below.
_loggers = set()

def get_logger(name, level=None, syslog_address="/dev/log"):
    """TODO"""

    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')

    # create singleton handlers
    if 'syslog' not in _log_handlers:
        _log_handlers['syslog'] = SysLogHandler(address=syslog_address)
        _log_handlers['syslog'].setFormatter(formatter)

    if 'stderr' not in _log_handlers:
        _log_handlers['stderr'] = StreamHandler()
        _log_handlers['stderr'].setFormatter(formatter)

    full_name = "heroshi" + ("."+name if name else "")
    logger = logging.getLogger(full_name)
    _loggers.add(logger)

    logger.addHandler(_log_handlers['syslog'])
    logger.addHandler(_log_handlers['stderr'])

    if level is None:
        try:
            logger.setLevel(settings.log['level'])
        except MissingOption:
            pass
    else:
        logger.setLevel(level)

    return logger

def update_loggers_level(level):
    """TODO"""

    # for all loggers created in future
    settings.log['level'] = level

    # for all already created loggers
    for logger in _loggers:
        logger.setLevel(level)

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception: # pylint: disable-msg=W0703
            get_logger("misc").exception("")
    return wrapper
