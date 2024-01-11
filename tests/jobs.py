# pyright: basic

import time
from unittest import TestCase
from src.daemonless_queuing import setup, shutdown
from .__fixtures__ import instance

enqueue = setup(instance, {
    'queues': [
        'Q1',
        'Q2'
    ]
})

class TestJobs(TestCase):
    def test_assertivity(self):
        limit = 5
        instance.delete('Q1_RESULT')

        for i in range(limit):
            enqueue('Q1', 'tests.__fixtures__.pong', None, i)

        time.sleep(6)

        for i in range(limit):
            result = int(instance.lpop('Q1_RESULT').decode())
            self.assertEqual(result, i)

    def test_timeout(self):
        enqueue('Q1', 'tests.__fixtures__.sleep', 3, 2, False)
        enqueue('Q1', 'tests.__fixtures__.sleep', 2, 3, True)
        enqueue('Q1', 'tests.__fixtures__.sleep', 2, 1, False)
        enqueue('Q1', 'tests.__fixtures__.sleep', 5, 10, True)
        time.sleep(11)

        for _ in range(4):
            result = instance.lpop('Q1_RESULT')
            if not result:
                continue
            self.assertEqual(result.decode(), 'False')

    @classmethod
    def tearDownClass(cls):
        shutdown()
