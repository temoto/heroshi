"""Configuration subsystem.

Reads config (YAML mappings)
from `path` list of files with fallback to os.environ[conf.ENVIRON_KEY].

Exposes config via `settings` attributes."""

from __future__ import with_statement
import os
import yaml

from heroshi.error import ConfigNotSpecified, ConfigNotFound, MissingOption


ENVIRON_KEY = "HEROSHI_CONFIG_PATH"


class Settings(object):
    """Syntax sugar to access settings via dot-syntax."""

    def __init__(self, config=None):
        self._target = config
        self._defaults = {}
        self._source = '' # from where config was read. For debugging purposes.

    def __getattr__(self, name):
        if name.startswith('__'):
            # helps against various introspecters who try to lookup attributes
            raise AttributeError(name)

        # loads settings if not already done
        if settings._target is None:
            init()
        if name in self._target:
            return self._target[name]
        if name in self._defaults:
            return self._defaults[name]
        raise MissingOption(name)

    def __contains__(self, name):
        # loads settings if not already done
        if settings._target is None:
            init()
        return name in self._target

    def get(self, name, default=None):
        """Convinient shortcut for .name-or-default."""

        try:
            return self.__getattr__(name)
        except MissingOption:
            return default

    def __setattr__(self, name, value):
        if name in ('_target', '_defaults', '_source'):
            self.__dict__[name] = value
        else:
            # this prevents get() from doing init()
            if settings._target is None:
                self._target = {}
            self._target[name] = value


def load_from_dict(new_config):
    """Atomically loads new settings from dict in `new_config`.

    Side-effect: global `conf.settings` is updated to new value."""

    settings._target = dict(new_config)
    settings._source = 'dict: %s' % repr(new_config)

def load_from_file(file_path):
    """Atomically loads new settings from file at `file_path`.

    Side-effect: global `conf.settings` is updated to new value."""

    with open(file_path) as f:
        new_config = yaml.safe_load(f)
    if new_config is None:
        new_config = {}
    load_from_dict(new_config)
    settings._source = "file: %s" % file_path

def init():
    """Loads first available config.

    First guess is value of environment key (value of `ENVIRON_KEY` constant).
    If that fails, seeks in list of locations in module attribute `path`.

    If all fails and source was not specified via environ or `path`, raises `ConfigNotSpecified`.
    If source was specified at least somehow but all paths don't exist, raises `ConfigNotFound`."""

    def first_good(xs, p):
        passed = filter(p, xs)
        return passed[0] if passed else None

    default_path = os.environ.get(ENVIRON_KEY)

    # Allow environment to create a stub empty config. Mostly useful for tests.
    if default_path and default_path.lower() in ("dummy", "stub"):
        settings._target = {}
        settings._source = "stub"
        return

    if default_path and default_path not in path:
        path.insert(0, default_path)

    existing = first_good(path, os.path.exists)
    if existing:
        return load_from_file(existing)
    else:
        # still not loaded
        if path:
            tries_msg = "Tried these: %s." % (", ".join( "'%s'" % (s,) for s in path ),)
            raise ConfigNotFound(tries_msg)
        else:
            tries_msg = "`conf.path` is empty and %s environ variable is not set." % (ENVIRON_KEY,)
            raise ConfigNotSpecified(tries_msg)
    # wrong branch guard
    assert False, u"This must not be executed."


# module attributes
settings = Settings()
# path is list of filesystem paths to search config for
# kinda conforms sys.path
path = []
