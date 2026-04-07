"""Extract AI diffusion data from the Microsoft AI Diffusion Report 2025 H2 PDF (appendix, pages 14-17)."""

import re

import pandas as pd
import pdfplumber
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Pages in the PDF that contain the appendix table (0-indexed).
APPENDIX_PAGES = [13, 14, 15, 16]

# Regex to match a data row: country name followed by three percentage values.
# Handles negative values (e.g. "-0.10%") and country names with parentheses (e.g. "Congo (DRC)").
ROW_PATTERN = re.compile(
    r"^(.+?)\s+([-\d]+\.\d+)%\s+([-\d]+\.\d+)%\s+([-\d]+\.\d+)%$"
)

# Lines to skip (page headers / section titles).
SKIP_LINES = {
    "Global AI Adoption in 2025 — A Widening Digital Divide",
    "Appendix",
    "Economy H1 2025 AI Diffusion H2 2025 AI Diffusion Change",
}


def extract_rows_from_page(page) -> list[dict]:
    text = page.extract_text() or ""
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line in SKIP_LINES or line.isdigit():
            continue
        m = ROW_PATTERN.match(line)
        if m:
            rows.append(
                {
                    "economy": m.group(1).strip(),
                    "ai_diffusion_h1_2025": float(m.group(2)),
                    "ai_diffusion_h2_2025": float(m.group(3)),
                }
            )
    return rows


def run() -> None:
    snap = paths.load_snapshot("ai_diffusion_msft.pdf")

    rows = []
    with pdfplumber.open(snap.path) as pdf:
        for page_num in APPENDIX_PAGES:
            rows.extend(extract_rows_from_page(pdf.pages[page_num]))

    assert len(rows) > 100, f"Expected >100 economies, got {len(rows)}"

    tb = Table(pd.DataFrame(rows), metadata=snap.to_table_metadata())
    for col in ["ai_diffusion_h1_2025", "ai_diffusion_h2_2025"]:
        tb[col].metadata.origins = [snap.metadata.origin]

    tb = tb.format(["economy"], short_name="ai_diffusion_msft")

    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
