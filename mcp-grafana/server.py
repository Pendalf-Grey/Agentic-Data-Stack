import base64
import json
import os
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

from fastmcp import FastMCP


mcp = FastMCP(name="ads-grafana")


def _settings() -> Dict[str, str]:
    return {
        "url": os.getenv("GRAFANA_URL", "http://grafana:3000").rstrip("/"),
        "public_url": os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3001").rstrip("/"),
        "username": os.getenv("GRAFANA_USERNAME", "admin"),
        "password": os.getenv("GRAFANA_PASSWORD", "admin"),
    }


def _request(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Any:
    settings = _settings()
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    auth = base64.b64encode(f"{settings['username']}:{settings['password']}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        f"{settings['url']}{path}",
        data=body,
        method=method,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _external_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{_settings()['public_url']}{path}"


@mcp.tool()
def ads2_dashboard(request: str) -> str:
    """Create or update the ADS-2 Grafana dashboard and return its public URL."""
    template_path = Path(os.getenv("ADS_GRAFANA_DASHBOARD_TEMPLATE", "/workspace/grafana/dashboards/agentic-data-stack-events.json"))
    dashboard = json.loads(template_path.read_text(encoding="utf-8"))
    result = _request("POST", "/api/dashboards/db", {"dashboard": dashboard, "overwrite": True})
    return _external_url(result.get("url", f"/d/{dashboard.get('uid', 'ads2-log-analysis')}"))


if __name__ == "__main__":
    mcp.run(
        transport=os.getenv("ADS_MCP_TRANSPORT", "sse"),
        host=os.getenv("ADS_MCP_BIND_HOST", "0.0.0.0"),
        port=int(os.getenv("ADS_MCP_PORT", "8000")),
    )
