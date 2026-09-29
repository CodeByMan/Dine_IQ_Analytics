"""Tests for the submission uptime evidence helper."""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
import unittest

from scripts.monitor_uptime import availability_percent, probe


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


class UptimeMonitorTests(unittest.TestCase):
    def test_probe_records_local_http_success(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            passed, detail = probe(f"http://127.0.0.1:{server.server_port}/", 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.assertTrue(passed)
        self.assertEqual(detail, "200")

    def test_availability_calculation_checks_ninety_nine_percent_target(self):
        self.assertEqual(availability_percent(99, 100), 99.0)
        self.assertLess(availability_percent(98, 100), 99.0)


if __name__ == "__main__":
    unittest.main()
