import typing
import json
import time
from threading import Thread
from multiprocessing import Process
from queue import Queue
from .function import get_function, parse_params_string
from .types import Redis

Options = typing.TypedDict('Options', {
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

def make_worker(channel: str, input_queue: WorkerQueue):
    while True:
        job = input_queue.get()
        function = get_function(job['function'])
        args, kwargs = parse_params_string(job['params'])

        def fn():
            if args and kwargs: function(*args, **kwargs)
            elif args: function(*args)
            elif kwargs: function(**kwargs)
            else: function()

        child = Process(target=fn)
        child.start()
        child.join()

        time.sleep(1)

def on_pub(msg: SubscriptionMessage, instance: Redis):
    event = msg['data'].decode()
    channel = ':'.join(msg['channel'].decode().split(':')[-2:])

    if event != 'rpush':
        return

    if channel not in Workers:
        iq: WorkerQueue = Queue()
        worker = Workers[channel] = typing.cast(Worker, {
            'thread': Thread(target=make_worker, args=(channel, iq)),
            'input_queue': iq
        })

        worker['thread'].start()

    if tail := instance.lpop(channel):
        job = typing.cast(Job, json.loads(tail))
        worker = Workers[channel]
        worker['input_queue'].put(job)

def make_enqueue(instance: Redis):
    def enqueue(queue: str, path: str, *args: typing.Any, **kwargs: typing.Any):
        job = {
            'function': path,
            'args': args,
            'kwargs': kwargs
        }

        return instance.rpush(json.dumps(job))

    return enqueue


def setup(instance: Redis, options: Options):
    instance.config_set('notify-keyspace-events', 'Kl')

    def pub_listener(msg: SubscriptionMessage):
        return on_pub(msg, instance)

    pubsub = instance.pubsub()
    pubsub.psubscribe(**{
        '__keyspace@0__:QUEUE:%s' % queue: pub_listener
        for queue in options['queues']
    })

    pubsub.run_in_thread(sleep_time=.1)
    return make_enqueue(instance)
