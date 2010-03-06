import os, logging



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

def get_logger():
    return logging.getLogger("heroshi")

def init_logging(syslog_address='/dev/log', level=None):
    from logging.handlers import SysLogHandler
    from logging import StreamHandler
    #create logger which will log at files mountman.log and to syslog logger at the same time
    syslog = SysLogHandler(address=syslog_address)
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    syslog.setFormatter(formatter)
    logger = get_logger()
    logger.addHandler(syslog)
    logger.addHandler(StreamHandler())
    if level is not None:
        logger.setLevel(level)
    return logger

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception: # pylint: disable-msg=W0703
            get_logger().exception("")
    return wrapper
