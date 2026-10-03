"""Load a snapshot and extract Table S8 (Country Statistics) from Meijer et al. (2021)."""

import re

import numpy as np
import pandas as pd
import pdfplumber
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run() -> None:
    """Extract Table S8 from the PDF and create a meadow dataset."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("meijer_2021.pdf")

    #
    # Process data.
    #
    log.info("Extracting Table S8 from PDF")
    tb = extract_table_s8(snap.path)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country"], short_name=paths.short_name)
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save changes in the new meadow dataset.
    ds_meadow.save()


# numeric tokens like: 28,486 | 1.0E-02 | 2.24% | 80
_NUM_TOKEN_RE = re.compile(
    r"""^[-+]?(
            (\d{1,3}(,\d{3})+|\d+)(\.\d+)?     # 1,234 or 1234.56
            |
            \d+(\.\d+)?[eE][-+]?\d+           # 1.0E-02
        )%?$""",
    re.VERBOSE,
)


def _is_num_token(tok: str) -> bool:
    tok = tok.strip()
    return bool(tok) and bool(_NUM_TOKEN_RE.match(tok))


def _count_num_tokens(s: str) -> tuple[int, bool]:
    toks = s.split()
    num_count = sum(_is_num_token(t) for t in toks)
    has_percent = any(t.endswith("%") and _is_num_token(t) for t in toks)
    return num_count, has_percent


def _parse_row(line: str):
    """
    Parse a single row by grabbing the last 9 numeric tokens:
    area, coast, rainfall, factor(L/A), factor((L/A)*P), P(E)%, MPW, M(E), ratio%
    """
    toks = line.split()
    vals = []
    idx = len(toks) - 1
    while idx >= 0 and len(vals) < 9:
        if _is_num_token(toks[idx]):
            vals.append(toks[idx])
        idx -= 1
    if len(vals) < 9:
        return None

    vals = list(reversed(vals))

    # remove the extracted numeric tokens from the right, leaving the country name
    remaining = toks.copy()
    for v in reversed(vals):
        for j in range(len(remaining) - 1, -1, -1):
            if remaining[j] == v and _is_num_token(remaining[j]):
                remaining.pop(j)
                break

    country = " ".join(remaining).strip()
    if not country:
        return None
    return [country] + vals


def _to_num(x):
    s = str(x).replace(",", "").replace("%", "")
    try:
        return float(s)
    except Exception:
        return np.nan


def extract_table_s8(pdf_path: str, start_page: int = 25, end_page: int = 29) -> Table:
    """
    Extract Table S8 from the supplementary PDF.
    Pages are 1-indexed (as in the paper PDF viewer): default pages 25–29.

    Returns a Table with numeric columns.
    """
    # pdfplumber pages are 0-indexed
    page_idxs = range(start_page - 1, end_page)

    # collect and normalize lines across pages
    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for i in page_idxs:
            txt = pdf.pages[i].extract_text(x_tolerance=2, y_tolerance=2) or ""
            for raw in txt.splitlines():
                line = " ".join(raw.strip().split())
                if line:
                    lines.append(line)

    rows = []
    buffer = ""
    started = False

    for line in lines:
        # Skip common header/legend lines (we also ignore everything until the first real data row)
        if (
            line.startswith("Table S8")
            or line.startswith("surface area")
            or line.startswith("ratios")
            or line.startswith("ratio M")
            or line.startswith("waste generation")
            or line.startswith("Country or")
            or line.startswith("administrative area")
            or line.startswith("Area [km2]")
            or line in {"E", "E E", "[E]"}
        ):
            continue

        num_count, has_percent = _count_num_tokens(line)

        # Don’t start parsing until we hit the first actual data row (has >= 9 numeric tokens incl. %)
        if not started:
            if num_count >= 9 and has_percent:
                started = True
            else:
                continue

        # If we have a buffered multi-line country row and the new line looks like a fresh full row,
        # try to finalize the buffer first.
        if buffer:
            b_num, b_has_pct = _count_num_tokens(buffer)
            if num_count >= 9 and has_percent and b_num >= 9 and b_has_pct:
                r = _parse_row(buffer)
                if r is not None:
                    rows.append(r)
                buffer = ""

        # Full row => parse directly
        if num_count >= 9 and has_percent:
            r = _parse_row(line)
            if r is not None:
                rows.append(r)
            else:
                buffer = (buffer + " " + line).strip()
            continue

        # Otherwise accumulate continuation lines (multi-line country names, etc.)
        buffer = (buffer + " " + line).strip()
        b_num, b_has_pct = _count_num_tokens(buffer)
        if b_num >= 9 and b_has_pct:
            r = _parse_row(buffer)
            if r is not None:
                rows.append(r)
                buffer = ""

    cols = [
        "country",
        "area_km2",
        "coast_length_km",
        "rainfall_mm_per_year",
        "factor_L_over_A",
        "factor_L_over_A_times_P",
        "P_E_percent",
        "MPW_tons_per_year",
        "ME_tons_per_year",
        "ME_over_MPW_percent",
    ]
    df = pd.DataFrame(rows, columns=cols)

    # numeric conversion
    for c in cols[1:]:
        df[c] = df[c].map(_to_num)

    # basic cleanup
    df["country"] = df["country"].str.replace(r"\s+", " ", regex=True).str.strip()

    tb = Table(df)
    return tb
