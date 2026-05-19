import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from metrics import render_metrics


# HTTP exporter для Prometheus: /metrics возвращает синтетические метрики, /health проверяет контейнер.

PORT = int(os.getenv("PORT", "9201"))


class SyntheticExporterHandler(BaseHTTPRequestHandler):
    """Минимальный HTTP handler для synthetic exporter."""

    def do_GET(self):
        if self.path == "/health":
            payload = json.dumps({"ok": True, "runtime": "python"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/metrics":
            payload = render_metrics(datetime.now(timezone.utc).replace(tzinfo=None), False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        payload = json.dumps({"error": "Not found"}).encode()
        self.send_response(404)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), SyntheticExporterHandler)
    print(f"synthetic-exporter listening on 0.0.0.0:{PORT}")
    server.serve_forever()
