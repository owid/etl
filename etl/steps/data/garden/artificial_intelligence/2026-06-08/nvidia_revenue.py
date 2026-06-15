"""Garden step for NVIDIA's quarterly revenue by market segment.

The snapshot is a faithful copy of NVIDIA's published "Revenue by Market" PDF
cells — multiple PDFs, two different segment taxonomies (NVIDIA recast its
disclosure in Q1 FY27), and the same quarter often appears in several PDFs.
This step harmonises that input into a single tidy time series for grapher:

- Segments are mapped into a unified scheme: data centers and AI vs. a single
  "Gaming, devices, automotive" bucket (the latter combining either the four
  old non-Data-Center segments, or NVIDIA's new "Edge Computing" line). The
  Data Center sub-rows from the new PDF (Hyperscale, AI Clouds, Industrial &
  Enterprise) are dropped — they're a sub-split of Data Center, not separate
  top-level segments.
- The quarter label (e.g. "Q4 FY26") is converted to NVIDIA's reported
  fiscal-quarter end date (the last Sunday of Apr/Jul/Oct/Jan). For example,
  Q4 FY26 -> 2026-01-25.
- When a quarter appears in more than one source PDF, the most recent PDF wins.
- Revenue is converted from millions to dollars.
"""

import calendar
import re
from datetime import date, timedelta

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map NVIDIA's raw segment names (as printed in their PDFs) to the unified
# two-line scheme used in the chart. Hyperscale and the "AI Clouds, Industrial,
# & Enterprise" sub-row are intentionally absent: they're a breakdown of the
# Data Center segment, not separate top-level segments.
SEG_DATA_CENTER = "Data centers and AI"
SEG_OTHER = "Gaming, devices, automotive"
SEG_TOTAL = "Total"

SEGMENT_MAP = {
    "Data Center": SEG_DATA_CENTER,
    "Datacenter": SEG_DATA_CENTER,
    "Gaming": SEG_OTHER,
    "Professional Visualization": SEG_OTHER,
    "Auto": SEG_OTHER,
    "Automotive": SEG_OTHER,
    "OEM & Other": SEG_OTHER,
    "OEM & IP": SEG_OTHER,
    "Edge Computing": SEG_OTHER,
    "TOTAL": SEG_TOTAL,
    "Total": SEG_TOTAL,
}


def _parse_quarter(label: str) -> tuple[int, int]:
    """Parse 'Q4 FY26' or 'Q4FY26' into (fiscal_quarter, fiscal_year_4digit)."""
    m = re.match(r"Q(\d)\s*FY(\d{2})", label)
    if not m:
        raise ValueError(f"Cannot parse quarter label: {label!r}")
    return int(m.group(1)), 2000 + int(m.group(2))


def _fiscal_quarter_end(fiscal_year: int, fiscal_quarter: int) -> date:
    """Last Sunday of NVIDIA's fiscal quarter end month (Apr/Jul/Oct/Jan)."""
    quarter_end_month = {1: 4, 2: 7, 3: 10, 4: 1}
    month = quarter_end_month[fiscal_quarter]
    cal_year = fiscal_year if fiscal_quarter == 4 else fiscal_year - 1
    last_day = calendar.monthrange(cal_year, month)[1]
    d = date(cal_year, month, last_day)
    while d.weekday() != 6:  # 6 = Sunday
        d -= timedelta(days=1)
    return d


def run() -> None:
    ds_meadow = paths.load_dataset("nvidia_revenue")
    tb = ds_meadow.read("nvidia_revenue")

    # Map each raw segment to its unified bucket; drop sub-segments not in the
    # map (Hyperscale, "AI Clouds, Industrial, & Enterprise").
    tb = tb[tb["segment"].isin(SEGMENT_MAP.keys())]
    tb["segment"] = tb["segment"].replace(SEGMENT_MAP)

    # Sum within (source_pdf, quarter, segment) so the four old non-Data-Center
    # segments collapse into one row per source PDF (and "Edge Computing" in the
    # new PDF simply passes through).
    tb = tb.groupby(["source_pdf", "quarter", "segment"], as_index=False, observed=True)["revenue_millions"].sum()

    # Deduplicate across source PDFs: same quarter often appears in many PDFs;
    # the most recent PDF (highest fiscal_year, then fiscal_quarter) wins.
    src_fyq = tb["source_pdf"].astype(str).apply(_parse_quarter)
    tb["_src_rank"] = src_fyq.apply(lambda t: t[1] * 10 + t[0])
    tb = tb.sort_values(["quarter", "segment", "_src_rank"], ascending=[True, True, False])
    tb = tb.drop_duplicates(subset=["quarter", "segment"], keep="first")
    tb = tb.drop(columns=["_src_rank", "source_pdf"])

    # Convert quarter label to fiscal-quarter-end date.
    qy = tb["quarter"].astype(str).apply(_parse_quarter)
    tb["date"] = qy.apply(lambda t: _fiscal_quarter_end(t[1], t[0]))
    tb = tb.drop(columns=["quarter"])

    # Millions of USD -> USD.
    tb["revenue_millions"] = tb["revenue_millions"] * 1_000_000
    tb = tb.rename(columns={"revenue_millions": "revenue"})

    tb = tb[["date", "segment", "revenue"]].sort_values(["date", "segment"]).reset_index(drop=True)
    tb = tb.format(["date", "segment"])

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
