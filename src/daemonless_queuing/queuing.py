import typing
import json
import time
import importlib
import re
from traceback import print_exc as xp
from datetime import datetime
from threading import Thread
from queue import Queue
from .types import Redis

Options = typing.TypedDict('Options', {
    'listen': bool,
    'queues': list[str]
})

SubscriptionMessage = typing.TypedDict('SubscriptionMessage', {
    'data': bytes,
    'channel': bytes
})

Job = typing.TypedDict('Job', {
    'function': typing.Callable[[typing.Any], typing.Any],
    'args': list[typing.Any],
    'kwargs': dict[str, typing.Any]
})

WorkerQueue = Queue[typing.Any]

Worker = typing.TypedDict('Worker', {
    'thread': Thread,
    'input_queue': WorkerQueue
})

Workers: dict[str, Worker] = {}
Threads: list[Thread] = []

PubsubThread: typing.Any = None

def queue_name(queue: str):
    return 'QUEUE:%s' % queue

def get_function(path: str):
    *_, func_name = path.split('.')
    module_name = '.'.join(_)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)


def make_worker(channel: str, input_queue: WorkerQueue):
    while True:
        job = input_queue.get()
        if job == -1:
            break

        try:
            function = get_function(job['function'])
            args, kwargs = job['args'], job['kwargs']

            def fn():
                if args and kwargs: function(*args, **kwargs)
                elif args: function(*args)
                elif kwargs: function(**kwargs)
                else: function()

            fn()

        except:
            xp()

        time.sleep(1)

def cast_datetime(obj: dict[str, typing.Any]):
    for k, v in obj.items():
        if isinstance(v, str) and re.search(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', v):
            try:
                obj[k] = datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
            except:
                pass
    return obj

def on_pub(msg: SubscriptionMessage, instance: Redis):
    global Threads

    event = msg['data'].decode()
    channel = ':'.join(msg['channel'].decode().split(':')[-2:])

    if event != 'rpush':
        return

    if channel not in Workers:
        iq: WorkerQueue = Queue()
        thread = Thread(target=make_worker, args=(channel, iq))
        Threads += [thread]

        worker = Workers[channel] = typing.cast(Worker, {
            'thread': thread,
            'input_queue': iq
        })

        worker['thread'].start()

    if tail := instance.lpop(channel):
        job = typing.cast(Job, json.loads(tail, object_hook=cast_datetime))
        worker = Workers[channel]
        worker['input_queue'].put(job)


def make_enqueue(instance: Redis, options: Options):
    def enqueue(queue: str, path: str, *args: typing.Any, **kwargs: typing.Any):
        if queue not in options['queues']:
            raise ValueError('invalid queue: "%s"' % queue)

        job = {
            'function': path,
            'args': args,
            'kwargs': kwargs
        }

        return instance.rpush(queue_name(queue), json.dumps(job, default=str))

    return enqueue

def setup(instance: Redis, options: Options):
    if options.get('listen', True):
        global PubsubThread
        instance.config_set('notify-keyspace-events', 'Kl')

        def pub_listener(msg: SubscriptionMessage):
            return on_pub(msg, instance)

        pubsub = instance.pubsub()
        pubsub.psubscribe(**{
            '__keyspace@0__:QUEUE:%s' % queue: pub_listener
            for queue in options['queues']
        })

        PubsubThread = pubsub.run_in_thread(sleep_time=.1)

    return make_enqueue(instance, options)

def shutdown():
    global PubsubThread
    PubsubThread.stop()

    for worker in Workers.values():
        worker['input_queue'].put(-1)

    for thread in Threads:
        thread.join()
