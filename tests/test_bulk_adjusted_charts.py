import unittest
import argparse
from types import SimpleNamespace
from unittest.mock import patch
import time
import requests
from backfills.bulk_adjusted_charts import batches, request_interval, slow_down_on_provider_error, BackfillKISClient
from collectors.kis_client import KISClient


class BulkAdjustedChartsTest(unittest.TestCase):
    def test_rate_limit_sets_cooldown_and_never_accelerates_on_its_own(self):
        client = BackfillKISClient('dummy', 'dummy', max_retries=0, min_request_interval=0.75)
        response = requests.Response()
        response.status_code = 429
        response.headers['Retry-After'] = '120'
        response._content = b'{}'
        before = time.monotonic()
        client._slow_down(requests.HTTPError(response=response))
        self.assertGreaterEqual(client.last_request_at, before + 120)
        self.assertEqual(client.min_request_interval, 1)
        client.session.close()

    def test_error_slows_down_before_next_asset_not_at_batch_end(self):
        client = BackfillKISClient('dummy', 'dummy', max_retries=0, min_request_interval=0.75)
        with patch.object(KISClient, '_request', side_effect=RuntimeError('fake')) as method:
            with self.assertRaises(RuntimeError):
                client._request('GET', 'https://example.invalid')
            self.assertEqual(method.call_count, 1)
            self.assertEqual(client.min_request_interval, 1)
        client.session.close()

    def test_business_failure_also_slows_down_without_retry(self):
        client = BackfillKISClient('dummy', 'dummy', max_retries=0, min_request_interval=0.75)
        with patch.object(KISClient, 'get_daily_price_page', side_effect=RuntimeError('fake')) as method:
            with self.assertRaises(RuntimeError):
                client.get_daily_price_page('NAS', 'TEST', None)
            self.assertEqual(method.call_count, 1)
            self.assertEqual(client.min_request_interval, 1)
        client.session.close()

    def test_interval_bounds(self):
        for value in ('nan', 'inf', '0', '0.49', '5.01'):
            with self.assertRaises(argparse.ArgumentTypeError):
                request_interval(value)
        self.assertEqual(request_interval('0.5'), 0.5)
        self.assertEqual(request_interval('1'), 1)

    def test_provider_failure_slows_down_without_retry(self):
        client = SimpleNamespace(min_request_interval=0.5)
        slow_down_on_provider_error(client, {'a': {'errorType': 'KISAPIError'}})
        self.assertEqual(client.min_request_interval, 1)
        slow_down_on_provider_error(client, {})
        self.assertEqual(client.min_request_interval, 1)

    def test_data_failure_does_not_change_interval(self):
        client = SimpleNamespace(min_request_interval=0.5)
        slow_down_on_provider_error(client, {'a': {'errorType': 'ValueError'}})
        self.assertEqual(client.min_request_interval, 0.5)
        client.min_request_interval = 2
        slow_down_on_provider_error(client, {'a': {'errorType': 'HTTPError'}})
        self.assertEqual(client.min_request_interval, 2)

    def test_page_budget_and_order(self):
        targets=[{'symbol':str(i),'rows':15000} for i in range(3)]
        self.assertEqual([len(b) for b in batches(targets)], [1,1,1])
        self.assertEqual([t for b in batches(targets) for t in b],targets)

    def test_asset_budget_and_single_large_asset(self):
        self.assertEqual([len(b) for b in batches([{'rows':1}]*26)],[25,1])
        self.assertEqual([len(b) for b in batches([{'rows':30000},{'rows':1}])],[1,1])
        self.assertEqual(list(batches([])),[])
