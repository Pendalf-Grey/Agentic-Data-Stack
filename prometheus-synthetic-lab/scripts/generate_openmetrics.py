import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


# Генератор истории OpenMetrics для пакетной загрузки Prometheus.
# Он использует тот же Python-код метрик, что и live exporter.

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "synthetic-exporter" / "src"))

from metrics import render_metrics  # noqa: E402


def parse_datetime(value):
    """Парсит ISO дату из env HISTORY_END."""
    if not value:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def main():
    hours = int(os.getenv("HISTORY_HOURS", "72"))
    step_seconds = int(os.getenv("HISTORY_STEP_SECONDS", "60"))
    output = Path(os.getenv("OUTPUT", "data/openmetrics/synthetic.openmetrics")).resolve()
    end = parse_datetime(os.getenv("HISTORY_END"))
    start = end - timedelta(hours=hours)
    output.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    metric_headers = set()
    current = start
    while current <= end:
        rendered = render_metrics(current, True)
        for line in rendered.strip().splitlines():
            if line.startswith("# HELP") or line.startswith("# TYPE"):
                if line not in metric_headers:
                    metric_headers.add(line)
                    lines.append(line)
                continue
            lines.append(line)
        current += timedelta(seconds=step_seconds)

    lines.append("# EOF")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "hours": hours, "stepSeconds": step_seconds, "start": start.isoformat() + "Z", "end": end.isoformat() + "Z", "lines": len(lines)}, indent=2))


if __name__ == "__main__":
    main()
