import random

from ansible.compat.tests import unittest
from ansible.module_utils.cloud import exponential_backoff, \
    full_jitter_backoff


class ExponentialBackoffStrategyTestCase(unittest.TestCase):
    def test_no_retries(self):
        strategy = exponential_backoff(retries=0)
        result = list(strategy())
        self.assertEquals(result, [], 'list should be empty')

    def test_exponential_backoff(self):
        strategy = exponential_backoff(retries=5, delay=1, backoff=2)
        result = list(strategy())
        self.assertEquals(result, [1, 2, 4, 8, 16])


class FullJitterBackoffStrategyTestCase(unittest.TestCase):
    def test_no_retries(self):
        strategy = full_jitter_backoff(retries=0)
        result = list(strategy())
        self.assertEquals(result, [], 'list should be empty')

    def test_full_jitter(self):
        retries = 5
        seed = 1

        r = random.Random(seed)
        expected = [r.randint(0, 2**i) for i in range(0, retries)]

        strategy = full_jitter_backoff(
            retries=retries, delay=1, _random=random.Random(seed))
        result = list(strategy())

        self.assertEquals(result, expected)
