# pyright: basic

import time
from unittest import TestCase
from queue import Queue
from threading import Thread
from src.daemonless_queuing import make_lock, RedisLockException
from .__fixtures__ import instance

lock = make_lock(instance, 'ONGOING_OP')
q = Queue()

blocking_lock = make_lock(instance, 'ONGOING_OP', block=True)

def blocking_async_op():
    with blocking_lock():
        time.sleep(2)

def async_op():
    try:
        with lock():
            time.sleep(2)
            q.put('didnt raise')
    except RedisLockException:
        q.put('raised')

class TestLock(TestCase):
    def test_blocking(self):
        t1 = Thread(target=blocking_async_op)
        t2 = Thread(target=blocking_async_op)
        t1.start()
        t2.start()
        t1.join()
        t2.join()


    def test_raises(self):
        t1 = Thread(target=async_op)
        t2 = Thread(target=async_op)
        t1.start()
        time.sleep(.5)

        t2.start()

        r1 = q.get()
        r2 = q.get()

        t1.join()
        t2.join()

        self.assertEqual(r1, 'raised')
        self.assertEqual(r2, 'didnt raise')


    def test_doesnt_raise(self):
        t1 = Thread(target=async_op)
        t2 = Thread(target=async_op)
        t1.start()
        time.sleep(3)

        t2.start()

        r1 = q.get()
        r2 = q.get()

        t1.join()
        t2.join()

        self.assertEqual(r1, 'didnt raise')
        self.assertEqual(r2, 'didnt raise')

