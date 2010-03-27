"""Heroshi logging subsystem.

Usage::

    from heroshi.log import get_logger
    log = get_logger("foo")
    log.info("Hello World!")
"""

import os, logging
from logging.handlers import SysLogHandler
from logging import StreamHandler

from heroshi.conf import settings
from heroshi.error import ConfigNotSpecified, MissingOption, WrongOption


# Handlers cache is to open a single fd to write to syslog and stderr.
_log_handlers = {}

# Loggers cache allows to update all existing loggers' level
# with a single call of the function `update_loggers_level` below.
_loggers = set()


def get_logger(name, syslog_address="/dev/log"):
    """TODO"""

    formatter = logging.Formatter("%(name)s: %(levelname)s %(message)s")

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

    # default level
    level = logging.WARNING
    try:
        level = settings.log['level']
    except KeyError:
        raise MissingOption('log:level')
    except TypeError:
        raise WrongOption('log:level', settings.log, "Mapping with at least 'level' key")
    except ConfigNotSpecified:
        # Don't let any configuration problem ruin whole logging.
        # And log the problem.
        if name == "": # but only for root logger, i.e. once
            logger.warning("Config was not specified. Using default log level: %s.", level)
            settings.log = {}
    except MissingOption:
        # Config is fine, but logging level is not configured. Use default.
        pass

    logger.setLevel(level)

    return logger

def update_loggers_level(level):
    """TODO"""

    try:
        settings.log
    except (MissingOption, ConfigNotSpecified):
        settings.log = {}

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
            get_logger("log").exception("")
    return wrapper

