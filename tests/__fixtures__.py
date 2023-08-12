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

lock = make_lock(instance)

def test(*args: typing.Any, **kwargs: typing.Any):
    if lock_name := kwargs.get('lock'):
        with lock(lock_name):
            time.sleep(kwargs.get('sleep', 5))
            print(kwargs.get('msg', 'Test!'))
        return

    time.sleep(kwargs.get('sleep', 5))
    print(kwargs.get('msg', 'Test!'))

def pong(number: int):
    print('got: %d!' % number)
    instance.rpush('Q1_RESULT', number)
