import base64
import json
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


# Этот контейнер стоит между LibreChat и локальным или внешним model backend.
# Поток данных: LibreChat -> llm-gateway -> model API -> llm-gateway -> LibreChat.
# Если включен Langfuse, proxy дополнительно отправляет trace/generation в Langfuse.

PORT = int(os.getenv("PORT", "3344"))
UPSTREAM_BASE_URL = os.getenv("UPSTREAM_MODEL_BASE_URL", "http://host.docker.internal:11434/v1").rstrip("/")
UPSTREAM_API_KEY = os.getenv("UPSTREAM_MODEL_API_KEY", "local-dev-key")
LANGFUSE_ENABLED = os.getenv("LANGFUSE_ENABLED", "false").lower() == "true"
LANGFUSE_BASE_URL = os.getenv("LANGFUSE_BASE_URL", "").rstrip("/")
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "")
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT", "local")
REASONING_EFFORT = os.getenv("LLM_GATEWAY_REASONING_EFFORT", "none").strip()
REASONING_EFFORT_MODELS = [
    item.strip().lower()
    for item in os.getenv("LLM_GATEWAY_REASONING_EFFORT_MODELS", "qwen3").split(",")
    if item.strip()
]


def utc_now_iso():
    """Возвращает ISO timestamp с миллисекундами для Langfuse."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_bytes(value):
    """Сериализует Python-объект в JSON bytes для HTTP-ответа или upstream-запроса."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def parse_json_body(handler):
    """Читает JSON body от LibreChat. Пустое тело превращается в пустой dict."""
    length = int(handler.headers.get("content-length") or 0)
    raw = handler.rfile.read(length) if length else b""
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def should_set_reasoning_effort(model):
    """Проверяет, нужно ли отключить долгий reasoning для локальной reasoning-модели."""
    model_name = str(model or "").lower()
    return bool(REASONING_EFFORT and any(model_name.startswith(prefix) for prefix in REASONING_EFFORT_MODELS))


def prepare_upstream_body(body):
    """Добавляет backend-параметры, которые LibreChat сам не передает."""
    if not isinstance(body, dict):
        return body
    prepared = dict(body)
    if "reasoning_effort" not in prepared and should_set_reasoning_effort(prepared.get("model")):
        prepared["reasoning_effort"] = REASONING_EFFORT
    return prepared


def send_json(handler, status, value):
    """Возвращает JSON-ответ клиенту LibreChat."""
    payload = json_bytes(value)
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    if handler.command != "HEAD":
        handler.wfile.write(payload)


def upstream_request(path, method="GET", body=None, stream=False):
    """Отправляет запрос в model backend и возвращает HTTP response object."""
    headers = {"Authorization": f"Bearer {UPSTREAM_API_KEY}"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json_bytes(body)
    request = Request(f"{UPSTREAM_BASE_URL}{path}", data=data, headers=headers, method=method)
    return urlopen(request, timeout=None if stream else 120)


def usage_from_completion(data):
    """Переводит usage ответа model backend в формат Langfuse."""
    usage = data.get("usage") if isinstance(data, dict) else None
    if not usage:
        return None
    return {
        "input": usage.get("prompt_tokens"),
        "output": usage.get("completion_tokens"),
        "total": usage.get("total_tokens"),
        "unit": "TOKENS",
    }


def extract_completion_text(data):
    """Достает текст assistant-ответа из non-streaming model backend ответа."""
    if not isinstance(data, dict):
        return ""
    parts = []
    for choice in data.get("choices") or []:
        message = choice.get("message") or {}
        delta = choice.get("delta") or {}
        text = message.get("content") or delta.get("content") or ""
        if text:
            parts.append(text)
    return "\n".join(parts)


def parse_streaming_content(chunk):
    """Достает текстовые delta из SSE stream, чтобы отправить их в Langfuse после ответа."""
    output = []
    for line in chunk.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[6:].strip()
        if not payload or payload == "[DONE]":
            continue
        try:
            output.append(json.loads(payload).get("choices", [{}])[0].get("delta", {}).get("content", ""))
        except json.JSONDecodeError:
            pass
    return "".join(output)


def send_langfuse_trace(body, output, usage, started_at, ended_at, status):
    """Отправляет trace в Langfuse. Ошибка Langfuse не ломает ответ пользователю."""
    if not (LANGFUSE_ENABLED and LANGFUSE_BASE_URL and LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY):
        return

    trace_id = str(uuid.uuid4())
    generation_id = str(uuid.uuid4())
    now = utc_now_iso()
    auth = base64.b64encode(f"{LANGFUSE_PUBLIC_KEY}:{LANGFUSE_SECRET_KEY}".encode()).decode()
    model = body.get("model") or "unknown-model"

    batch = [
        {
            "id": str(uuid.uuid4()),
            "type": "trace-create",
            "timestamp": now,
            "body": {
                "id": trace_id,
                "timestamp": started_at,
                "name": "librechat.llm.request",
                "input": body.get("messages") or body,
                "output": output,
                "environment": LANGFUSE_ENVIRONMENT,
                "tags": ["agentic-data-stack", "librechat", "llm-gateway"],
                "metadata": {"upstreamBaseUrl": UPSTREAM_BASE_URL, "stream": bool(body.get("stream")), "status": status},
            },
        },
        {
            "id": str(uuid.uuid4()),
            "type": "generation-create",
            "timestamp": now,
            "body": {
                "id": generation_id,
                "traceId": trace_id,
                "name": "chat.completions",
                "startTime": started_at,
                "endTime": ended_at,
                "model": model,
                "modelParameters": {
                    "temperature": body.get("temperature"),
                    "top_p": body.get("top_p"),
                    "max_tokens": body.get("max_tokens"),
                    "stream": bool(body.get("stream")),
                },
                "input": body.get("messages") or body,
                "output": output,
                "usage": usage,
                "level": "ERROR" if status >= 400 else "DEFAULT",
                "statusMessage": f"Upstream returned HTTP {status}" if status >= 400 else None,
                "metadata": {"endpoint": "/v1/chat/completions"},
            },
        },
    ]
    payload = {"batch": batch, "metadata": {"source": "agentic-data-stack-llm-gateway"}}
    try:
        request = Request(
            f"{LANGFUSE_BASE_URL}/api/public/ingestion",
            data=json_bytes(payload),
            headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=3) as response:
            if response.status not in (200, 207):
                print(f"Langfuse ingestion returned HTTP {response.status}")
    except Exception as error:
        print(f"Langfuse ingestion skipped: {error}")


class AgentProxyHandler(BaseHTTPRequestHandler):
    """HTTP handler: принимает endpoints /health, /v1/models и /v1/chat/completions."""

    server_version = "agentic-data-stack-llm-gateway-python/1.0"

    def log_message(self, fmt, *args):
        """Оставляем стандартный access log компактным."""
        print(f"{self.address_string()} - {fmt % args}")

    def do_GET(self):
        """Обрабатывает health check и список моделей."""
        if self.path == "/health":
            send_json(self, 200, {"ok": True, "upstreamBaseUrl": UPSTREAM_BASE_URL, "runtime": "python"})
            return
        if self.path == "/v1/models":
            try:
                with upstream_request("/models") as response:
                    payload = response.read()
                    self.send_response(response.status)
                    self.send_header("Content-Type", response.headers.get("content-type", "application/json"))
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
            except HTTPError as error:
                send_json(self, error.code, json.loads(error.read().decode("utf-8") or "{}"))
            return
        send_json(self, 404, {"error": "Not found"})

    def do_POST(self):
        """Проксирует chat completions, включая streaming SSE."""
        if self.path != "/v1/chat/completions":
            send_json(self, 404, {"error": "Not found"})
            return
        started_at = utc_now_iso()
        try:
            body = prepare_upstream_body(parse_json_body(self))
            with upstream_request("/chat/completions", method="POST", body=body, stream=bool(body.get("stream"))) as response:
                if not body.get("stream"):
                    raw = response.read()
                    data = json.loads(raw.decode("utf-8") or "{}")
                    send_json(self, response.status, data)
                    send_langfuse_trace(
                        body,
                        extract_completion_text(data) or data,
                        usage_from_completion(data),
                        started_at,
                        utc_now_iso(),
                        response.status,
                    )
                    return

                self.send_response(response.status)
                self.send_header("Content-Type", response.headers.get("content-type", "text/event-stream"))
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                streamed_output = []
                while True:
                    # SSE от Ollama приходит строками `data: ...`.
                    # Если читать крупными блоками через read(8192), proxy может ждать закрытия
                    # upstream-соединения даже после `data: [DONE]`, а LibreChat видит `terminated`.
                    line = response.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace")
                    streamed_output.append(parse_streaming_content(text))
                    self.wfile.write(line)
                    self.wfile.flush()
                    if text.strip() == "data: [DONE]":
                        break
                send_langfuse_trace(
                    body,
                    "".join(streamed_output),
                    None,
                    started_at,
                    utc_now_iso(),
                    response.status,
                )
        except Exception as error:
            traceback.print_exc()
            send_json(self, 500, {"error": {"message": str(error), "type": "llm_gateway_error"}})


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), AgentProxyHandler)
    print(f"llm-gateway listening on 0.0.0.0:{PORT}")
    print(f"upstream model base URL: {UPSTREAM_BASE_URL}")
    print(f"langfuse tracing: {'enabled' if LANGFUSE_ENABLED else 'disabled'}")
    server.serve_forever()
