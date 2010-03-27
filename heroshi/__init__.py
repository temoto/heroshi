__author__ = u"Sergey Shepelev"
__email__ = "temotor@gmail.com"
__version__ = "0.3"
__status__ = "Development"

from .log import get_logger, log_exceptions
get_logger("heroshi") # root logger. Just to initialize logging subsystem.
from .profile import profile

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

