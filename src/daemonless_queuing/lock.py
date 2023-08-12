import typing
import contextlib
from functools import reduce
from .types import Redis

class RedisLockException(Exception):
    def __init__(self, lock_name: str | list[str]):
        self.lock_name = lock_name
    def __str__(self):
        return 'lock exception: {}'.format(self.lock_name)

def make_lock(instance: Redis):
    @contextlib.contextmanager
    def use_lock(key: str | list[str]):
        def treat_list(action: str, *args: typing.Any) -> None:
            if isinstance(key, list):
                result: list[bool] = []
                for k in key:
                    result.append(getattr(instance, action)(k, *args))
                return reduce(lambda a, b : a and b, typing.cast(typing.Any, result))

            return getattr(instance, action)(key, *args)

        if treat_list('get'):
            raise RedisLockException(key)

        treat_list('set', 1)
        treat_list('expire', 60*60*2)
        try:
            yield
        finally:
            treat_list('delete')

    return use_lock
