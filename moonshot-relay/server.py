import gzip
import http.client
import http.server
import json
import socket
import socketserver


UPSTREAM_HOST = "api.moonshot.ai"
UPSTREAM_TIMEOUT_SECONDS = 90
UPSTREAM_ATTEMPTS = 1
HOP_BY_HOP = {
    "accept-encoding",
    "connection",
    "content-encoding",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
RESPONSE_HOP_BY_HOP = HOP_BY_HOP - {"content-encoding"}


class RelayHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        print(f"{self.client_address[0]} {self.command} {self.path} {fmt % args}", flush=True)

    def do_GET(self):
        if self.path == "/health":
            payload = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        self._relay(compress=False)

    def do_POST(self):
        self._relay(compress=True)

    def _relay(self, compress):
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        client_stream = False
        if compress and body:
            try:
                payload = json.loads(body)
                client_stream = payload.get("stream") is True
                if client_stream:
                    payload["stream"] = False
                    payload.pop("stream_options", None)
                    body = json.dumps(
                        payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
            except (TypeError, ValueError):
                pass
        if compress and body:
            body = gzip.compress(body, compresslevel=9)

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP
        }
        headers["Host"] = UPSTREAM_HOST
        headers["Accept-Encoding"] = "identity"
        headers["Connection"] = "close"
        headers["Content-Length"] = str(len(body))
        if compress and body:
            headers["Content-Encoding"] = "gzip"

        try:
            last_error = None
            for attempt in range(1, UPSTREAM_ATTEMPTS + 1):
                upstream = http.client.HTTPSConnection(
                    UPSTREAM_HOST,
                    443,
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                )
                try:
                    upstream.request(self.command, self.path, body=body, headers=headers)
                    response = upstream.getresponse()
                    response_body = response.read()
                    response_status = response.status
                    response_reason = response.reason
                    upstream_headers = response.getheaders()
                    print(
                        f"upstream attempt={attempt} request_bytes={len(body)} "
                        f"status={response_status} response_bytes={len(response_body)}",
                        flush=True,
                    )
                    break
                except (TimeoutError, socket.timeout, http.client.HTTPException, OSError) as error:
                    last_error = error
                    print(
                        f"upstream retry attempt={attempt} request_bytes={len(body)} "
                        f"error={type(error).__name__}",
                        flush=True,
                    )
                finally:
                    upstream.close()
            else:
                raise RuntimeError(
                    f"Moonshot request failed after {UPSTREAM_ATTEMPTS} attempts: {last_error}"
                ) from last_error

            if client_stream and 200 <= response_status < 300:
                response_body = self._to_sse(response_body)
                response_headers = {
                    "Content-Type": "text/event-stream; charset=utf-8",
                    "Cache-Control": "no-cache",
                }
            else:
                response_headers = {
                    key: value
                    for key, value in upstream_headers
                    if key.lower() not in RESPONSE_HOP_BY_HOP
                    and key.lower() != "content-length"
                }

            self.send_response(response_status, response_reason)
            for key, value in response_headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(response_body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response_body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            try:
                self.send_error(502, str(error))
            except Exception:
                pass
        finally:
            self.close_connection = True

    @staticmethod
    def _to_sse(response_body):
        completion = json.loads(response_body)
        choices = completion.get("choices", [])
        common = {
            "id": completion.get("id"),
            "object": "chat.completion.chunk",
            "created": completion.get("created"),
            "model": completion.get("model"),
        }
        if completion.get("system_fingerprint") is not None:
            common["system_fingerprint"] = completion["system_fingerprint"]

        content_chunk = {
            **common,
            "choices": [
                {
                    "index": choice.get("index", 0),
                    "delta": choice.get("message") or {},
                    "finish_reason": None,
                }
                for choice in choices
            ],
        }
        finish_chunk = {
            **common,
            "choices": [
                {
                    "index": choice.get("index", 0),
                    "delta": {},
                    "finish_reason": choice.get("finish_reason"),
                }
                for choice in choices
            ],
        }
        if completion.get("usage") is not None:
            finish_chunk["usage"] = completion["usage"]

        events = [content_chunk, finish_chunk]
        encoded = b"".join(
            b"data: "
            + json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n\n"
            for event in events
        )
        return encoded + b"data: [DONE]\n\n"


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


ThreadingServer(("0.0.0.0", 18080), RelayHandler).serve_forever()
