"""Snapshot of NVIDIA's quarterly Revenue-by-Market PDFs.

Downloads every "Revenue Trends by Market" PDF NVIDIA Investor Relations publishes,
extracts the table on its first page, and writes one tidy CSV row per source-PDF /
reported-quarter / segment cell. Segment names and quarter labels are preserved
exactly as NVIDIA prints them; no aggregation or relabelling happens here. Any
harmonisation across PDF presentations belongs in meadow/garden.
"""

import io
import re
from pathlib import Path

import click
import pandas as pd
import pdfplumber
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

SNAPSHOT_VERSION = Path(__file__).parent.name

# Source PDFs published by NVIDIA Investor Relations. Each PDF reports several
# quarters; we keep historical PDFs for depth and the latest PDF for new data.
PDF_URLS = {
    "Q1FY27": "https://s201.q4cdn.com/141608511/files/doc_financials/2027/Q127/Rev_by_Mkt_Qtrly_Trend_Q127-NEW-v3.pdf",
    "Q4FY26": "https://s201.q4cdn.com/141608511/files/doc_financials/2026/Q426/Rev_by_Mkt_Qtrly_Trend_Q426.pdf",
    "Q3FY26": "https://s201.q4cdn.com/141608511/files/doc_financials/2026/Q326/Rev_by_Mkt_Qtrly_Trend_Q326.pdf",
    "Q4FY25": "https://s201.q4cdn.com/141608511/files/doc_financials/2025/Q425/Rev_by_Mkt_Qtrly_Trend_Q425.pdf",
    "Q4FY24": "https://s201.q4cdn.com/141608511/files/doc_financials/2024/Q4FY24/Rev_by_Mkt_Qtrly_Trend_Q424.pdf",
    "Q4FY23": "https://s201.q4cdn.com/141608511/files/doc_financials/2023/Q423/Q423-Qtrly-Revenue-by-Market-slide.pdf",
    "Q4FY22": "https://s201.q4cdn.com/141608511/files/doc_financials/2022/q4/Rev_by_Mkt_Qtrly_Trend_Q422.pdf",
    "Q4FY21": "https://s201.q4cdn.com/141608511/files/doc_financials/annual/2021/Rev_by_Mkt_Qtrly_Trend_Q421.pdf",
    "Q4FY20": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2020/Q420/Rev_by_Mkt_Qtrly_Trend_Q420.pdf",
    "Q4FY19": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2019/Q419/Rev_by_Mkt_Qtrly_Trend_Q419.pdf",
    "Q4FY18": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2018/Rev_by_Mkt_Qtrly_Trend_Q418.pdf",
    "Q4FY17": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2017/Rev_by_Mkt_Qtrly_Trend_Q417.pdf",
    "Q4FY16": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2016/Rev_by_Mkt_Qtrly_Trend_Q416.FINAL.pdf",
}

# First source PDF that uses NVIDIA's recast market-platform presentation (in
# which the four non-Data-Center segments are rolled into a single "Edge
# Computing" line, and Data Center is sub-split into Hyperscale + ACIE).
NEW_PRESENTATION_FROM = (2027, 1)  # (fiscal_year, fiscal_quarter)


def _parse_source_label(label: str) -> tuple[int, int]:
    m = re.match(r"Q(\d)FY(\d{2})", label)
    if not m:
        raise ValueError(f"Cannot parse PDF source label: {label!r}")
    return int(m.group(1)), 2000 + int(m.group(2))


def _uses_new_presentation(label: str) -> bool:
    q, y = _parse_source_label(label)
    return (y, q) >= NEW_PRESENTATION_FROM


def _download_pdf(url: str) -> bytes:
    log.info("downloading_pdf", url=url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def _clean_value(value) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace("$", "").replace(",", "").replace(" ", "")
    if s in {"", "-", "—"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _clean_label(name) -> str:
    """Strip footnote markers and collapse whitespace in a row/column label."""
    s = str(name).replace("\n", " ").strip()
    s = re.sub(r"[\d\*†‡]+$", "", s).strip()
    return re.sub(r"\s+", " ", s)


def _extract_old_format(pdf_bytes: bytes, source: str) -> list[dict]:
    """Old PDFs (Q4 FY16 .. Q4 FY26): rows = segment, columns = quarter ('Q1 FY26')."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables() or []

        table = None
        for cand in tables:
            if len(cand) > 5 and len(cand[0]) > 2:
                header = " ".join(str(c or "") for c in cand[0])
                if any(q in header for q in ["Q1", "Q2", "Q3", "Q4"]):
                    table = cand
                    break
        if table is None and tables:
            table = tables[0]
        if table is None:
            log.warning("no_table_found", source=source)
            return []

        # Some older PDFs concatenate all values for a row into a single cell.
        if len(table) > 1 and len(table[1]) > 1:
            none_count = sum(1 for cell in table[1][1:] if cell is None)
            if none_count > len(table[1]) / 2:
                fixed = [table[0]]
                for row in table[1:]:
                    if isinstance(row[1], str):
                        values = row[1].replace("$", "").replace(",", "").strip().split()
                        fixed.append([row[0]] + values)
                    else:
                        fixed.append(row)
                table = fixed

        df = pd.DataFrame(table[1:], columns=table[0])

        # Some PDFs (Q4 FY21) lose segment names from rows whose label wraps
        # across two lines. Reconstruct by listing the canonical segment order
        # and filling nan rows with the ones missing from the table — skipping
        # any segments already labelled to avoid double-assignment.
        if df.iloc[:, 0].isna().any():
            text = page.extract_text() or ""
            seg_order = ["Gaming", "Professional Visualization", "Data Center", "Auto", "OEM & Other", "TOTAL"]
            already_labelled = {str(s) for s in df.iloc[:, 0].dropna()}
            pending = [s for s in seg_order if s not in already_labelled and s in text]
            for nan_idx, seg in zip(df.index[df.iloc[:, 0].isna()].tolist(), pending):
                df.iloc[nan_idx, 0] = seg

    records: list[dict] = []
    seg_col = df.columns[0]
    for _, row in df.iterrows():
        seg = row[seg_col]
        if pd.isna(seg):
            continue
        segment = _clean_label(seg)
        if not segment:
            continue
        for col in df.columns[1:]:
            m = re.match(r"Q(\d)\s*FY(\d{2})", str(col))
            if not m:
                continue
            value = _clean_value(row[col])
            if value is None:
                continue
            records.append(
                {
                    "source_pdf": source,
                    "quarter": f"Q{m.group(1)} FY{m.group(2)}",
                    "segment": segment,
                    "revenue_millions": value,
                }
            )
    return records


def _extract_new_format(pdf_bytes: bytes, source: str) -> list[dict]:
    """New PDF (Q1 FY27+): two-row header with fiscal-year span over quarter labels."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables() or []

    table = None
    for cand in tables:
        flat = "\n".join(" ".join(str(c or "") for c in row) for row in cand)
        if "Fiscal" in flat and "TOTAL" in flat:
            table = cand
            break
    if table is None or len(table) < 3:
        log.warning("no_table_found", source=source)
        return []

    header_fy = table[0]
    header_q = table[1]
    data_rows = table[2:]

    col_fyq: list[tuple[int, int] | None] = []
    current_fy: int | None = None
    for fy_cell, q_cell in zip(header_fy[1:], header_q[1:]):
        if fy_cell and str(fy_cell).strip():
            m = re.search(r"\d{4}", str(fy_cell))
            if m:
                current_fy = int(m.group())
        if current_fy is None:
            col_fyq.append(None)
            continue
        m = re.match(r"Q(\d)", str(q_cell or "").strip())
        if not m:
            col_fyq.append(None)
            continue
        col_fyq.append((current_fy, int(m.group(1))))

    records: list[dict] = []
    for row in data_rows:
        if not row or not row[0]:
            continue
        segment = _clean_label(row[0])
        if not segment:
            continue
        for idx, fyq in enumerate(col_fyq):
            if fyq is None:
                continue
            cell = row[idx + 1] if idx + 1 < len(row) else None
            value = _clean_value(cell)
            if value is None:
                continue
            fy, fq = fyq
            records.append(
                {
                    "source_pdf": source,
                    "quarter": f"Q{fq} FY{str(fy)[-2:]}",
                    "segment": segment,
                    "revenue_millions": value,
                }
            )
    return records


def extract_nvidia_revenue() -> pd.DataFrame:
    """Download every source PDF and return a tidy long table.

    Output columns: source_pdf, quarter, segment, revenue_millions. Each row is
    one cell from a source PDF; segment names are as NVIDIA prints them.
    Downstream (garden) is responsible for any aggregation, relabelling, and
    deduplication across PDFs.
    """
    log.info("starting_extraction", total_pdfs=len(PDF_URLS))
    rows: list[dict] = []
    for source, url in PDF_URLS.items():
        try:
            pdf_bytes = _download_pdf(url)
        except Exception as e:
            log.error("download_failed", source=source, error=str(e))
            continue
        if _uses_new_presentation(source):
            rows.extend(_extract_new_format(pdf_bytes, source))
        else:
            rows.extend(_extract_old_format(pdf_bytes, source))

    if not rows:
        raise ValueError("No data extracted from any PDF")

    df = pd.DataFrame(rows).sort_values(["source_pdf", "quarter", "segment"]).reset_index(drop=True)
    log.info("extraction_complete", rows=len(df), sources=df["source_pdf"].nunique(), segments=df["segment"].nunique())
    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/nvidia_revenue.csv")
    df = extract_nvidia_revenue()
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
