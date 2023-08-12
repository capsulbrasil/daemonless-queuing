import redis
from src.daemonless_queuing import setup

instance = redis.Redis(
    host='localhost',
    port=6379
)

def start():
    setup(instance, {
        'queues': [
            'TESTCHAN_1',
            'TESTCHAN_2',
            'FAST'
        ]
    })

    while True:
        continue

start()
