from __future__ import annotations

import re
from pathlib import Path

from docx import Document
from docx.enum.text import WD_BREAK
from docx.shared import Inches, Pt, RGBColor


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "JUNIOR_DEVOPS_DEPLOYMENT_GUIDE.md"
TXT_OUT = ROOT / "docs" / "JUNIOR_DEVOPS_DEPLOYMENT_GUIDE.txt"
DOCX_OUT = ROOT / "docs" / "JUNIOR_DEVOPS_DEPLOYMENT_GUIDE.docx"


def clean_inline_markdown(text: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    text = text.replace("**", "")
    text = text.replace("__", "")
    text = text.replace("`", "")
    return text.rstrip()


def paragraph_with_inline_code(document: Document, text: str, style: str | None = None):
    paragraph = document.add_paragraph(style=style)
    parts = re.split(r"(`[^`]+`)", text)
    for part in parts:
        if not part:
            continue
        if part.startswith("`") and part.endswith("`"):
            run = paragraph.add_run(part[1:-1])
            run.font.name = "Courier New"
            run.font.size = Pt(10)
            run.font.color.rgb = RGBColor(20, 20, 20)
        else:
            run = paragraph.add_run(part.replace("**", "").replace("__", ""))
            run.font.name = "Arial"
            run.font.size = Pt(11)
    return paragraph


def markdown_to_txt(markdown: str) -> str:
    lines: list[str] = []
    in_code = False

    for raw in markdown.splitlines():
        line = raw.rstrip()

        if line.startswith("```"):
            in_code = not in_code
            lines.append("")
            continue

        if in_code:
            lines.append(line)
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading:
            level = len(heading.group(1))
            title = clean_inline_markdown(heading.group(2))
            if lines and lines[-1] != "":
                lines.append("")
            lines.append(title.upper() if level == 1 else title)
            lines.append("=" * min(len(title), 80) if level <= 2 else "-" * min(len(title), 80))
            continue

        if re.match(r"^\s*[-*]\s+", line):
            lines.append(re.sub(r"^\s*[-*]\s+", "- ", clean_inline_markdown(line)))
            continue

        if re.match(r"^\s*\d+\.\s+", line):
            lines.append(clean_inline_markdown(line))
            continue

        lines.append(clean_inline_markdown(line))

    text = "\n".join(lines)
    text = re.sub(r"\n{4,}", "\n\n\n", text)
    return text.strip() + "\n"


def markdown_to_docx(markdown: str):
    document = Document()
    section = document.sections[0]
    section.top_margin = Inches(0.75)
    section.bottom_margin = Inches(0.75)
    section.left_margin = Inches(0.8)
    section.right_margin = Inches(0.8)

    styles = document.styles
    styles["Normal"].font.name = "Arial"
    styles["Normal"].font.size = Pt(11)
    styles["Title"].font.name = "Arial"
    styles["Title"].font.size = Pt(22)
    styles["Heading 1"].font.name = "Arial"
    styles["Heading 1"].font.size = Pt(16)
    styles["Heading 2"].font.name = "Arial"
    styles["Heading 2"].font.size = Pt(14)
    styles["Heading 3"].font.name = "Arial"
    styles["Heading 3"].font.size = Pt(12)

    footer = section.footer.paragraphs[0]
    footer.alignment = 2
    footer.add_run("Agentic Data Stack deployment guide")

    in_code = False
    code_buffer: list[str] = []

    def flush_code():
        nonlocal code_buffer
        if not code_buffer:
            return
        paragraph = document.add_paragraph()
        paragraph.paragraph_format.space_before = Pt(3)
        paragraph.paragraph_format.space_after = Pt(6)
        for index, code_line in enumerate(code_buffer):
            if index:
                paragraph.add_run().add_break(WD_BREAK.LINE)
            run = paragraph.add_run(code_line)
            run.font.name = "Courier New"
            run.font.size = Pt(8.5)
        code_buffer = []

    for raw in markdown.splitlines():
        line = raw.rstrip()

        if line.startswith("```"):
            if in_code:
                flush_code()
            in_code = not in_code
            continue

        if in_code:
            code_buffer.append(line)
            continue

        if not line.strip():
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading:
            title = clean_inline_markdown(heading.group(2))
            level = len(heading.group(1))
            if level == 1:
                document.add_paragraph(title, style="Title")
            else:
                document.add_heading(title, level=min(level, 3))
            continue

        if line.startswith("> "):
            paragraph = paragraph_with_inline_code(document, line[2:])
            for run in paragraph.runs:
                run.italic = True
            continue

        if re.match(r"^\s*[-*]\s+", line):
            text = re.sub(r"^\s*[-*]\s+", "", line)
            paragraph_with_inline_code(document, text, style="List Bullet")
            continue

        if re.match(r"^\s*\d+\.\s+", line):
            text = re.sub(r"^\s*\d+\.\s+", "", line)
            paragraph_with_inline_code(document, text, style="List Number")
            continue

        paragraph_with_inline_code(document, line)

    flush_code()
    return document


def main() -> None:
    markdown = SOURCE.read_text(encoding="utf-8")
    TXT_OUT.write_text(markdown_to_txt(markdown), encoding="utf-8")
    document = markdown_to_docx(markdown)
    document.save(DOCX_OUT)
    print(TXT_OUT)
    print(DOCX_OUT)


if __name__ == "__main__":
    main()
