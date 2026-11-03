import unittest
from unittest.mock import Mock, patch

import requests
from collectors.kis_client import KISClient
from collectors.kis_diagnostics import error_metadata


class KISDiagnosticsTest(unittest.TestCase):
    def response_error(self, status, code=None):
        response = requests.Response()
        response.status_code = status
        response.headers['Retry-After'] = '60'
        response.json = Mock(return_value={'msg_cd': code, 'msg1': 'SECRET', 'token': 'SECRET'})
        return requests.HTTPError('SECRET request headers', response=response)

    def test_zero_retry_returns_response_without_hidden_retry_wrapper(self):
        client = KISClient('dummy', 'dummy', max_retries=0)
        for adapter in client.session.adapters.values():
            self.assertEqual(adapter.max_retries.total, 0)
            self.assertFalse(adapter.max_retries.raise_on_status)
        client.session.close()

    def test_positive_retry_behavior_unchanged(self):
        client = KISClient('dummy', 'dummy', max_retries=2)
        self.assertTrue(client.session.adapters['https://'].max_retries.raise_on_status)
        client.session.close()

    def test_failed_request_updates_pacing_timestamp(self):
        client = KISClient('dummy', 'dummy', max_retries=0, min_request_interval=0)
        with patch.object(client.session, 'request', side_effect=requests.ConnectionError('SECRET')):
            with self.assertRaises(requests.ConnectionError):
                client._request('GET', 'https://example.invalid')
        self.assertGreater(client.last_request_at, 0)
        client.session.close()

    def test_classifies_http_and_business_rate_limit(self):
        self.assertEqual(error_metadata(self.response_error(429))['failureCategory'], 'RATE_LIMIT')
        result = error_metadata(self.response_error(500, 'EGW00201'))
        self.assertEqual(result['failureCategory'], 'RATE_LIMIT')
        self.assertEqual(result['retryAfterSeconds'], 60)
        self.assertEqual(error_metadata(ValueError('KIS daily price failed: EGW00201 text'))['failureCategory'], 'RATE_LIMIT')

    def test_5xx_is_not_automatically_a_rate_limit(self):
        result = error_metadata(self.response_error(503, 'EGW_UNKNOWN'))
        self.assertEqual(result['failureCategory'], 'PROVIDER_5XX')
        self.assertEqual(error_metadata(self.response_error(403))['failureCategory'], 'HTTP_OTHER')

    def test_no_secrets_or_untrusted_codes_are_recorded(self):
        result = error_metadata(self.response_error(500, 'SECRET token=123'))
        self.assertNotIn('SECRET', str(result))
        self.assertIsNone(result['providerErrorCode'])
        self.assertEqual(error_metadata(requests.exceptions.RetryError('SECRET'))['failureCategory'], 'UNCLASSIFIED')

    def test_non_json_response_is_safe(self):
        error = self.response_error(502)
        error.response.json.side_effect = ValueError('SECRET')
        self.assertEqual(error_metadata(error)['httpStatus'], 502)

    def test_oversized_retry_header_does_not_mask_original_error(self):
        error = self.response_error(429)
        error.response.headers['Retry-After'] = '9' * 5000
        result = error_metadata(error)
        self.assertEqual(result['failureCategory'], 'RATE_LIMIT')
        self.assertIsNone(result['retryAfterSeconds'])


if __name__ == '__main__':
    unittest.main()
