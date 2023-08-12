# Daemonless Queuing

Simple and reliable Redis queuing utilities.

## Install

```sh
$ pip install daemonless-queuing
```

## Usage

### Queuing

```python
import redis
from daemonless_queuing import setup, shutdown

instance = redis.Redis(
    host='localhost',
    port=6379
)

enqueue = setup(instance, {
    'queues': [
        'TESTCHAN_1',
        'TESTCHAN_2'
    ]
})

enqueue('TESTCHAN_1', 'my_package.my_module.func_name')
enqueue('TESTCHAN_2', 'my_package.my_module.func_name', 'positional argument', 42, named_arg='Hey!')
# ...

# do some blocking stuff

shutdown()
```

### Locking

```python
import redis
from daemonless_queuing import make_lock

instance = redis.Redis(
    host='localhost',
    port=6379
)

lock = make_lock(instance, 'SOMETHING_ONGOING')

with lock():
    # will raise if you try to run this scope again before the lock gets released
    ...
```

## Why?

I couldn't find a better suiting solution for what I needed (enqueuing stuff in multiple queues then running the jobs in parallel). Existing solutions ([rq](https://python-rq.org/), [Celery](https://docs.celeryq.dev/en/stable/)) are hard to setup and require a daemon/broker to work. Of course this is a much simpler version of these libraries, but some don't need more than that.


## Caveats

- Functions must be provided as Python import paths
- Function parameters must be JSON-serializable
- Return values can't be accessed

## Race condition

The function returned by `make_lock` is susceptible to race condition at the moment.
Make sure your asynchronous function won't be called twice at the same time.


## License

This library is [MIT licensed](https://github.com/capsulbrasil/normalize-json/tree/master/LICENSE).
