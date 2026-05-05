from __future__ import annotations

import re
from pathlib import Path

from docx import Document
from docx.enum.section import WD_ORIENT
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
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


def is_table_row(line: str) -> bool:
    return bool(re.match(r"^\s*\|.*\|\s*$", line))


def is_table_separator(line: str) -> bool:
    cells = parse_table_row(line)
    return bool(cells) and all(re.match(r"^:?-{3,}:?$", cell.strip()) for cell in cells)


def parse_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def set_cell_shading(cell, fill: str) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    shading = tc_pr.find(qn("w:shd"))
    if shading is None:
        shading = OxmlElement("w:shd")
        tc_pr.append(shading)
    shading.set(qn("w:fill"), fill)


def set_cell_border(cell, color: str = "D9DEE8", size: str = "6") -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    borders = tc_pr.first_child_found_in("w:tcBorders")
    if borders is None:
        borders = OxmlElement("w:tcBorders")
        tc_pr.append(borders)
    for edge in ("top", "left", "bottom", "right"):
        tag = f"w:{edge}"
        element = borders.find(qn(tag))
        if element is None:
            element = OxmlElement(tag)
            borders.append(element)
        element.set(qn("w:val"), "single")
        element.set(qn("w:sz"), size)
        element.set(qn("w:space"), "0")
        element.set(qn("w:color"), color)


def set_cell_margins(cell, margin: int = 90) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    margins = tc_pr.first_child_found_in("w:tcMar")
    if margins is None:
        margins = OxmlElement("w:tcMar")
        tc_pr.append(margins)
    for side in ("top", "left", "bottom", "right"):
        element = margins.find(qn(f"w:{side}"))
        if element is None:
            element = OxmlElement(f"w:{side}")
            margins.append(element)
        element.set(qn("w:w"), str(margin))
        element.set(qn("w:type"), "dxa")


def set_cell_width(cell, width_dxa: int) -> None:
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_width = tc_pr.first_child_found_in("w:tcW")
    if tc_width is None:
        tc_width = OxmlElement("w:tcW")
        tc_pr.append(tc_width)
    tc_width.set(qn("w:w"), str(width_dxa))
    tc_width.set(qn("w:type"), "dxa")


def repeat_table_header(row) -> None:
    tr_pr = row._tr.get_or_add_trPr()
    repeat = OxmlElement("w:tblHeader")
    repeat.set(qn("w:val"), "true")
    tr_pr.append(repeat)


def set_table_width(table, width_dxa: int) -> None:
    tbl_pr = table._tbl.tblPr
    tbl_width = tbl_pr.find(qn("w:tblW"))
    if tbl_width is None:
        tbl_width = OxmlElement("w:tblW")
        tbl_pr.append(tbl_width)
    tbl_width.set(qn("w:w"), str(width_dxa))
    tbl_width.set(qn("w:type"), "dxa")

    layout = tbl_pr.find(qn("w:tblLayout"))
    if layout is None:
        layout = OxmlElement("w:tblLayout")
        tbl_pr.append(layout)
    layout.set(qn("w:type"), "fixed")


def column_widths(headers: list[str], page_width_dxa: int) -> list[int]:
    weights_by_header = {
        "Машина": 1.15,
        "VM": 0.95,
        "Компонент": 1.45,
        "Компоненты": 2.8,
        "Порт": 0.8,
        "Откуда Доступ": 1.55,
        "Для Чего": 2.45,
        "CPU": 0.75,
        "RAM": 0.95,
        "Disk": 1.0,
        "Комментарий": 2.25,
    }
    weights = [weights_by_header.get(header, 1.2) for header in headers]
    total = sum(weights)
    widths = [int(page_width_dxa * weight / total) for weight in weights]
    widths[-1] += page_width_dxa - sum(widths)
    return widths


def add_word_table(document: Document, rows: list[list[str]]) -> None:
    if len(rows) < 2:
        return
    headers = rows[0]
    body = rows[1:]
    usable_width_dxa = 10560
    widths = column_widths(headers, usable_width_dxa)

    table = document.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    table.autofit = False
    table.allow_autofit = False
    set_table_width(table, usable_width_dxa)
    repeat_table_header(table.rows[0])

    for column_index, header in enumerate(headers):
        cell = table.rows[0].cells[column_index]
        set_cell_width(cell, widths[column_index])
        set_cell_margins(cell, 100)
        set_cell_border(cell, "C6CCD8", "8")
        set_cell_shading(cell, "EEF2F7")
        cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        paragraph = cell.paragraphs[0]
        run = paragraph.add_run(clean_inline_markdown(header))
        run.bold = True
        run.font.name = "Arial"
        run.font.size = Pt(8.5)

    for row_values in body:
        cells = table.add_row().cells
        for column_index, value in enumerate(row_values):
            cell = cells[column_index]
            set_cell_width(cell, widths[column_index])
            set_cell_margins(cell, 100)
            set_cell_border(cell)
            cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER
            paragraph = cell.paragraphs[0]
            paragraph.paragraph_format.space_after = Pt(0)
            parts = re.split(r"(`[^`]+`)", value)
            for part in parts:
                if not part:
                    continue
                if part.startswith("`") and part.endswith("`"):
                    run = paragraph.add_run(part[1:-1])
                    run.font.name = "Courier New"
                    run.font.size = Pt(7.7)
                else:
                    run = paragraph.add_run(part.replace("**", "").replace("__", ""))
                    run.font.name = "Arial"
                    run.font.size = Pt(8)

    document.add_paragraph()


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
    section.orientation = WD_ORIENT.LANDSCAPE
    section.page_width = Inches(11)
    section.page_height = Inches(8.5)
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
    table_buffer: list[list[str]] = []

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

    def flush_table():
        nonlocal table_buffer
        if not table_buffer:
            return
        add_word_table(document, table_buffer)
        table_buffer = []

    for raw in markdown.splitlines():
        line = raw.rstrip()

        if line.startswith("```"):
            flush_table()
            if in_code:
                flush_code()
            in_code = not in_code
            continue

        if in_code:
            code_buffer.append(line)
            continue

        if not line.strip():
            flush_table()
            continue

        if is_table_row(line):
            if is_table_separator(line):
                continue
            table_buffer.append(parse_table_row(line))
            continue
        flush_table()

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

    flush_table()
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
