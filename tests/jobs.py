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
            enqueue('Q1', 'tests.__fixtures__.pong', i)

        time.sleep(6)

        for i in range(limit):
            result = int(instance.lpop('Q1_RESULT').decode())
            self.assertEqual(result, i)

        shutdown()
