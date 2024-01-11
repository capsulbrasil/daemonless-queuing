# pyright: basic

import typing
import time
import redis
import os
from src.daemonless_queuing import make_lock

instance = redis.Redis(
    host=str(os.getenv('REDIS_HOST')),
    port=int(str(os.getenv('REDIS_PORT')))
)


def test(*args: typing.Any, **kwargs: typing.Any):
    if lock_name := kwargs.get('lock'):
        lock = make_lock(instance, lock_name)
        with lock():
            time.sleep(kwargs.get('sleep', 5))
            print(kwargs.get('msg', 'Test!'))
        return

    time.sleep(kwargs.get('sleep', 5))
    print(kwargs.get('msg', 'Test!'))

def pong(number: int):
    print('got: %d!' % number)
    instance.rpush('Q1_RESULT', number)

def sleep(number: int, should: bool):
    time.sleep(number)
    instance.rpush('Q1_RESULT', str(should))
