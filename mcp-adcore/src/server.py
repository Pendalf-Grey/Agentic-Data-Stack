from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP


BASE_URL = os.getenv("ADCORE_BASE_URL", "").rstrip("/")
API_TOKEN = os.getenv("ADCORE_API_TOKEN", "")
USERNAME = os.getenv("ADCORE_USERNAME", "")
PASSWORD = os.getenv("ADCORE_PASSWORD", "")
DEFAULT_ACCESS_LEVELS = [
    item.strip()
    for item in os.getenv("ADCORE_DEFAULT_ACCESS_LEVELS", "readonly").split(",")
    if item.strip()
]
TIMEOUT_SECONDS = float(os.getenv("ADCORE_REQUEST_TIMEOUT_SECONDS", "60"))
VERIFY_TLS = os.getenv("ADCORE_VERIFY_TLS", "true").lower() not in {"0", "false", "no"}
MAX_RESPONSE_CHARS = int(os.getenv("ADCORE_MAX_RESPONSE_CHARS", "12000"))
MCP_HOST = os.getenv("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.getenv("MCP_PORT", "8000"))


app = FastMCP("adcore", host=MCP_HOST, port=MCP_PORT)


class AdcoreClient:
    def __init__(self) -> None:
        self._login_token: str | None = None
        self._login_token_expires_at = 0.0

    async def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        token = API_TOKEN or await self._login_token_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _login_token_value(self) -> str:
        if not USERNAME or not PASSWORD:
            return ""
        if self._login_token and time.time() < self._login_token_expires_at:
            return self._login_token

        payload: dict[str, str] = {"password": PASSWORD}
        if "@" in USERNAME:
            payload["email"] = USERNAME
        else:
            payload["username"] = USERNAME

        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, verify=VERIFY_TLS) as client:
            response = await client.post(f"{BASE_URL}/v1/auth/login", json=payload)
        response.raise_for_status()
        data = response.json()
        token = (
            data.get("token")
            or data.get("access_token")
            or data.get("jwt")
            or data.get("data", {}).get("token")
            or data.get("data", {}).get("access_token")
            or ""
        )
        self._login_token = token
        self._login_token_expires_at = time.time() + 10 * 60
        return token

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not BASE_URL:
            return {
                "ok": False,
                "status_code": None,
                "error": "ConfigError",
                "message": "ADCORE_BASE_URL is not set. Use the Tailscale URL of the external Adcore gateway.",
            }
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, verify=VERIFY_TLS) as client:
                response = await client.request(
                    method,
                    f"{BASE_URL}{path}",
                    params=params,
                    json=json_body,
                    headers=await self._headers(),
                )
            return {
                "ok": 200 <= response.status_code < 300,
                "status_code": response.status_code,
                "data": parse_response(response),
            }
        except Exception as exc:
            return {
                "ok": False,
                "status_code": None,
                "error": type(exc).__name__,
                "message": str(exc),
                "adcore_base_url": BASE_URL,
            }


client = AdcoreClient()


def parse_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return clamp(response.json())
        except json.JSONDecodeError:
            pass
    return clamp(response.text)


def clamp(value: Any) -> Any:
    rendered = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    if len(rendered) <= MAX_RESPONSE_CHARS:
        return value
    return {
        "truncated": True,
        "max_chars": MAX_RESPONSE_CHARS,
        "text": rendered[:MAX_RESPONSE_CHARS],
    }


@app.tool()
async def adcore_health() -> dict[str, Any]:
    """Check whether the external adcore server is reachable through GET /healthz."""
    return await client.request("GET", "/healthz")


@app.tool()
async def adcore_status() -> dict[str, Any]:
    """Read adcore runtime status through GET /v1/status."""
    return await client.request("GET", "/v1/status")


@app.tool()
async def adcore_openapi() -> dict[str, Any]:
    """Fetch adcore OpenAPI YAML through GET /v1/openapi.yaml."""
    return await client.request("GET", "/v1/openapi.yaml")


@app.tool()
async def adcore_list_sessions(limit: int = 20) -> dict[str, Any]:
    """List adcore chat/agent sessions."""
    return await client.request("GET", "/v1/chat/sessions", params={"limit": limit})


@app.tool()
async def adcore_create_session(title: str | None = None) -> dict[str, Any]:
    """Create an adcore chat/agent session."""
    body: dict[str, Any] = {}
    if title:
        body["title"] = title
    return await client.request("POST", "/v1/chat/sessions", json_body=body)


@app.tool()
async def adcore_get_messages(session_id: str, limit: int = 50) -> dict[str, Any]:
    """Read messages from an adcore chat/agent session."""
    return await client.request(
        "GET",
        "/v1/chat/messages",
        params={"session_id": session_id, "limit": limit},
    )


@app.tool()
async def adcore_send_task(
    message: str,
    session_id: str | None = None,
    access_levels: list[str] | None = None,
) -> dict[str, Any]:
    """Send a task to the external adcore agent through POST /v1/chat."""
    body: dict[str, Any] = {
        "text": message,
        "options": {
            "stream": False,
            "access_levels": access_levels if access_levels is not None else DEFAULT_ACCESS_LEVELS,
        },
    }
    if session_id:
        body["session_id"] = session_id
    return await client.request("POST", "/v1/chat", json_body=body)


if __name__ == "__main__":
    app.run(transport="sse")
