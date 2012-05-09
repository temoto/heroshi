# coding: utf-8
import pkg_resources

from .log import get_logger, log_exceptions
get_logger("heroshi") # root logger. Just to initialize logging subsystem.
from .profile import profile


try:
    # cache
    VERSION = pkg_resources.get_distribution("heroshi").version
except Exception:
    VERSION = "unknown"
__version__ = VERSION

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

