import typing
import contextlib
import time
from functools import reduce
from .types import Redis

class RedisLockException(Exception):
    def __init__(self, lock_name: str | list[str]):
        self.lock_name = lock_name
    def __str__(self):
        return 'lock exception: {}'.format(self.lock_name)

def make_lock(instance: Redis, key: str | list[str], block: bool = False):
    @contextlib.contextmanager
    def use_lock():
        def treat_list(action: str, *args: typing.Any) -> None:
            if isinstance(key, list):
                result: list[bool] = []
                for k in key:
                    result.append(getattr(instance, action)(k, *args))
                return reduce(lambda a, b : a and b, typing.cast(typing.Any, result))

            return getattr(instance, action)(key, *args)

        if treat_list('get'):
            if not block:
                raise RedisLockException(key)
            while treat_list('get'):
                time.sleep(1)

        treat_list('set', 1)
        try:
            yield
        finally:
            treat_list('delete')

    return use_lock
