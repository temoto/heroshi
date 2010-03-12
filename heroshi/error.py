"""Common exceptions."""


class Error(Exception):
    """Base class for all Heroshi errors."""

    def __init__(self, msg=None):
        self.msg = msg
        super(Error, self).__init__()

    def __unicode__(self):
        return u"<%s %s>" % (self.__class__.__name__, unicode(self.msg))

    def __str__(self):
        return str(unicode(self))

    def __repr__(self):
        return unicode(self)


class ConfigurationError(Error):
    """Base class for errors with server config."""
    pass


class MissingOption(ConfigurationError, AttributeError):
    """Required option is missing in config."""

    def __init__(self, option):
        super(MissingOption, self).__init__("Option \"%s\" is not defined in config." % option)


class WrongOption(ConfigurationError):
    """Option contains wrong value."""

    def __init__(self, option, value, expected):
        super(WrongOption, self).__init__(
                "Config option \"%s\" has wrong value \"%s\". %s expected." % (option, value, expected))


class ApiError(Error):
    pass


class CrawlError(Error):
    pass


class FetchError(CrawlError):
    pass


class RobotsError(CrawlError):
    pass
