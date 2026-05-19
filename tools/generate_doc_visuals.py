from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"
OUT.mkdir(parents=True, exist_ok=True)


PALETTE = {
    "ink": "#172033",
    "muted": "#5B6475",
    "paper": "#FFFFFF",
    "grid": "#DDE5F0",
    "blue": "#2563EB",
    "blue_soft": "#DBEAFE",
    "green": "#16A34A",
    "green_soft": "#DCFCE7",
    "amber": "#D97706",
    "amber_soft": "#FEF3C7",
    "red": "#DC2626",
    "red_soft": "#FEE2E2",
    "slate_soft": "#F8FAFC",
}


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    return ImageFont.load_default()


FONT_TITLE = font(34, True)
FONT_H2 = font(22, True)
FONT_BODY = font(18)
FONT_SMALL = font(15)
FONT_SMALL_BOLD = font(15, True)


def rounded(draw: ImageDraw.ImageDraw, xy, fill, outline="#CBD5E1", radius=18, width=2):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def text_center(draw: ImageDraw.ImageDraw, xy, text: str, fnt, fill=PALETTE["ink"]):
    left, top, right, bottom = xy
    bbox = draw.multiline_textbbox((0, 0), text, font=fnt, spacing=6, align="center")
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    draw.multiline_text(((left + right - w) / 2, (top + bottom - h) / 2), text, font=fnt, fill=fill, spacing=6, align="center")


def text_left(draw: ImageDraw.ImageDraw, xy, text: str, fnt, fill=PALETTE["ink"], spacing=5):
    draw.multiline_text(xy, text, font=fnt, fill=fill, spacing=spacing)


def arrow(draw: ImageDraw.ImageDraw, start, end, color=PALETTE["blue"], width=4):
    draw.line([start, end], fill=color, width=width)
    x1, y1 = start
    x2, y2 = end
    if abs(x2 - x1) >= abs(y2 - y1):
        direction = 1 if x2 >= x1 else -1
        points = [(x2, y2), (x2 - 14 * direction, y2 - 9), (x2 - 14 * direction, y2 + 9)]
    else:
        direction = 1 if y2 >= y1 else -1
        points = [(x2, y2), (x2 - 9, y2 - 14 * direction), (x2 + 9, y2 - 14 * direction)]
    draw.polygon(points, fill=color)


def draw_header(draw: ImageDraw.ImageDraw, title: str, subtitle: str):
    text_left(draw, (56, 36), title, FONT_TITLE, PALETTE["ink"])
    text_left(draw, (58, 82), subtitle, FONT_BODY, PALETTE["muted"])


def architecture_flow():
    img = Image.new("RGB", (1600, 900), PALETTE["paper"])
    draw = ImageDraw.Draw(img)
    draw_header(draw, "Agentic Data Stack: поток данных", "От source systems до dashboards, чата и LLM observability")

    boxes = [
        ((70, 180, 340, 325), "External DB\nPostgreSQL / MySQL\nMongoDB", PALETTE["blue_soft"], PALETTE["blue"]),
        ((425, 180, 695, 325), "Debezium\nCDC connectors", PALETTE["green_soft"], PALETTE["green"]),
        ((780, 180, 1050, 325), "Apache Kafka\nbroker\ntransport", PALETTE["amber_soft"], PALETTE["amber"]),
        ((1135, 180, 1450, 325), "ClickHouse Sink\nKafka Connect", PALETTE["green_soft"], PALETTE["green"]),
        ((1135, 405, 1450, 550), "ClickHouse\nanalytics +\nprometheus_samples", PALETTE["blue_soft"], PALETTE["blue"]),
        ((780, 405, 1050, 550), "MCP Server\nsafe data tools", PALETTE["slate_soft"], "#64748B"),
        ((425, 405, 695, 550), "LibreChat\nuser questions", PALETTE["slate_soft"], "#64748B"),
        ((70, 405, 340, 550), "Grafana\nhuman dashboards", PALETTE["amber_soft"], PALETTE["amber"]),
        ((70, 650, 340, 795), "Prometheus\nremote_write /\nquery_range", PALETTE["red_soft"], PALETTE["red"]),
        ((425, 650, 695, 795), "Prometheus\nConnector", PALETTE["red_soft"], PALETTE["red"]),
        ((780, 650, 1050, 795), "Langfuse\nLLM traces", PALETTE["green_soft"], PALETTE["green"]),
        ((1135, 650, 1450, 795), "Agent Proxy\nmodel gateway", PALETTE["blue_soft"], PALETTE["blue"]),
    ]

    for xy, label, fill, outline in boxes:
        rounded(draw, xy, fill, outline)
        text_center(draw, xy, label, FONT_H2)

    for start, end in [
        ((340, 252), (425, 252)),
        ((695, 252), (780, 252)),
        ((1050, 252), (1135, 252)),
        ((1292, 325), (1292, 405)),
        ((1135, 477), (1050, 477)),
        ((780, 477), (695, 477)),
        ((205, 550), (205, 610)),
        ((340, 722), (425, 722)),
        ((695, 722), (1135, 520)),
        ((695, 485), (340, 485)),
        ((695, 520), (1135, 722)),
        ((1135, 722), (1050, 722)),
    ]:
        arrow(draw, start, end)

    draw.rounded_rectangle((56, 840, 1544, 868), radius=14, fill=PALETTE["slate_soft"], outline=PALETTE["grid"])
    text_left(draw, (78, 844), "Читать слева направо: CDC-данные идут через Debezium/Apache Kafka, метрики Prometheus идут через connector, анализ и dashboards читают ClickHouse.", FONT_SMALL, PALETTE["muted"])
    img.save(OUT / "guide_architecture_flow.png")


def resource_profile():
    img = Image.new("RGB", (1600, 900), PALETTE["paper"])
    draw = ImageDraw.Draw(img)
    draw_header(draw, "Ресурсный профиль deployment-вариантов", "Ориентир для планирования ноутбука, mini-VM и production-like схемы")

    rows = [
        ("Laptop demo", 8, 22, 120, PALETTE["blue"]),
        ("3 local VM", 10, 26, 220, PALETTE["green"]),
        ("2 local VM", 10, 22, 200, PALETTE["amber"]),
        ("Production HA", 60, 180, 3500, PALETTE["red"]),
    ]
    metrics = [
        ("CPU cores / vCPU", 70),
        ("RAM GB", 220),
        ("Disk GB", 4000),
    ]

    y0 = 180
    row_h = 150
    for idx, (name, cpu, ram, disk, color) in enumerate(rows):
        y = y0 + idx * row_h
        rounded(draw, (70, y, 330, y + 105), PALETTE["slate_soft"], PALETTE["grid"], radius=14, width=1)
        text_center(draw, (70, y, 330, y + 105), name, FONT_H2)
        values = [cpu, ram, disk]
        for j, ((label, max_value), value) in enumerate(zip(metrics, values)):
            x = 410 + j * 365
            draw.text((x, y - 8), label, font=FONT_SMALL_BOLD, fill=PALETTE["muted"])
            draw.rounded_rectangle((x, y + 28, x + 295, y + 62), radius=17, fill="#EEF2F7")
            bar_w = int(295 * min(value / max_value, 1))
            draw.rounded_rectangle((x, y + 28, x + bar_w, y + 62), radius=17, fill=color)
            draw.text((x, y + 74), f"{value:g}", font=FONT_BODY, fill=PALETTE["ink"])

    text_left(draw, (70, 805), "Не цель угадать exact sizing, а быстро увидеть порядок величин и понять, когда all-in-one перестает быть удобным.", FONT_BODY, PALETTE["muted"])
    img.save(OUT / "guide_resource_profile.png")


def monitoring_map():
    img = Image.new("RGB", (1600, 900), PALETTE["paper"])
    draw = ImageDraw.Draw(img)
    draw_header(draw, "Monitoring map: что смотреть после запуска", "Карта сигналов для Grafana, LibreChat и ручной диагностики")

    groups = [
        ("Data ingest", "Debezium status\nKafka consumer lag\nClickHouse sink errors", PALETTE["blue_soft"], PALETTE["blue"]),
        ("Storage", "ClickHouse disk\nQuery latency\nFreshness of samples", PALETTE["green_soft"], PALETTE["green"]),
        ("Orchestration", "Airflow DAG failures\nSchedule state\nConnector registration", PALETTE["amber_soft"], PALETTE["amber"]),
        ("LLM app", "LibreChat health\nMCP health\nagent-proxy health", PALETTE["slate_soft"], "#64748B"),
        ("LLM observability", "Langfuse web/worker\nTrace volume\nIngestion errors", PALETTE["green_soft"], PALETTE["green"]),
        ("Prometheus path", "remote_write health\nBackfill results\nprometheus_samples rows", PALETTE["red_soft"], PALETTE["red"]),
    ]

    for i, (title, body, fill, outline) in enumerate(groups):
        col = i % 3
        row = i // 3
        x = 70 + col * 505
        y = 180 + row * 265
        rounded(draw, (x, y, x + 430, y + 190), fill, outline, radius=20, width=3)
        draw.text((x + 28, y + 25), title, font=FONT_H2, fill=PALETTE["ink"])
        text_left(draw, (x + 30, y + 72), body, FONT_BODY, PALETTE["muted"], spacing=9)

    rounded(draw, (310, 730, 1290, 810), PALETTE["blue_soft"], PALETTE["blue"], radius=20, width=2)
    text_center(draw, (310, 730, 1290, 810), "Хороший dashboard отвечает на три вопроса: что сломалось, где именно, когда началось.", FONT_H2)
    img.save(OUT / "guide_monitoring_map.png")


def main():
    architecture_flow()
    resource_profile()
    monitoring_map()
    for name in ("guide_architecture_flow.png", "guide_resource_profile.png", "guide_monitoring_map.png"):
        print(OUT / name)


if __name__ == "__main__":
    main()
