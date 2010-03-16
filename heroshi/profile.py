import time

from heroshi.misc import get_logger
log = get_logger("profile")


class Profile(object):
    def __init__(self, name):
        self.name = name
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_value, exc_tb):
        end = time.time()
        time_passed = end - self.start_time
        log.info(u"%s: %d ms", self.name, time_passed * 1000)
        return False

    def decorate(self):
        def wrapper(func):
            def wrapped(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)
            return wrapped
        return wrapper


def profile(name_or_func):
    if callable(name_or_func):
        # argument is function, act like decorator
        decorator = Profile(name_or_func.__name__).decorate()
        return decorator(name_or_func)
    else:
        # argument is name, return decorator
        return Profile(name_or_func).decorate()

