import typing
import time
import redis
from src.daemonless_queuing import make_lock

instance = redis.Redis(
    host='localhost',
    port=6379
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
