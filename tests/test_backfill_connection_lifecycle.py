"""Reproduce remote disconnect on reuse without an external API or credentials."""
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from backfills.bulk_adjusted_charts import BackfillKISClient
from collectors.kis_client import KISClient


class DropsReusedConnection(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.requests_on_connection = 0

    def do_GET(self):
        self.requests_on_connection += 1
        if self.requests_on_connection > 1:
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', '2')
        if self.headers.get('Connection', '').lower() == 'close':
            self.send_header('Connection', 'close')
            self.close_connection = True
        self.end_headers()
        self.wfile.write(b'{}')
        self.wfile.flush()

    def log_message(self, *args):
        pass


class BackfillConnectionLifecycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), DropsReusedConnection)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}/page'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def test_old_reuse_reproduces_disconnect_but_backfill_passes_every_request(self):
        old = KISClient('dummy', 'dummy', max_retries=0, min_request_interval=0)
        improved = BackfillKISClient('dummy', 'dummy', max_retries=0, min_request_interval=0)
        try:
            self.assertEqual(old._request('GET', self.url).status_code, 200)
            with self.assertRaises(requests.ConnectionError):
                old._request('GET', self.url)
            for _ in range(8):
                response = improved._request('GET', self.url)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {})
                self.assertEqual(response.headers['Connection'], 'close')
            for adapter in improved.session.adapters.values():
                self.assertEqual(adapter.max_retries.total, 0)
        finally:
            old.session.close()
            improved.session.close()


if __name__ == '__main__':
    unittest.main()
