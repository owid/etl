"""
Cherry blossom full-flowering dates in Kyoto, Japan (812–2025).

A 1200-year time series showing how climate change is pulling peak
cherry blossom earlier in the year.
"""

import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from owid.catalog import processing as pr
from owl.catalog import export, load_snapshot
from owl.dataset import Action, Dataset
from owl.snapshot import Snapshot

# ── Helpers ─────────────────────────────────────────────────────────

R2_BUCKET = "owid-snapshots"


def _fetch_from_r2(md5: str, suffix: str) -> Path:
    """Download a file from R2 by its md5 hash, return temp path."""
    prefix, rest = md5[:2], md5[2:]
    key = f"{prefix}/{rest}"
    tmp = Path(tempfile.mkdtemp()) / f"snapshot{suffix}"
    subprocess.run(
        ["rclone", "cat", f"r2:{R2_BUCKET}/{key}"],
        stdout=open(tmp, "wb"),
        check=True,
    )
    return tmp


# ── Snapshots ────────────────────────────────────────────────────────


@Snapshot(version="2024-01-25")
def historical_data():
    """Historical cherry blossom data (812–2015) from Aono's XLS."""
    path = _fetch_from_r2("fc459738bd4a0a73c716755d598c6678", ".xls")
    return pd.read_excel(path, skiprows=25)


@Snapshot(version="2025-04-07")
def recent_data():
    """Recent years (2016–2025) from personal communication with Aono."""
    path = _fetch_from_r2("92a4923dc15e29707f33f8eb589d236d", ".csv")
    return pd.read_csv(path)


# ── Dataset ──────────────────────────────────────────────────────────


@Dataset
def cherry_blossom(historical_data: Snapshot, recent_data: Snapshot):
    """Combine historical + recent data, convert dates, compute rolling average."""

    tb_hist = load_snapshot(historical_data, short_name="cherry_blossom")
    tb_recent = load_snapshot(recent_data, short_name="cherry_blossom_recent")

    # ── Clean historical data ────────────────────────────────────────
    tb_hist = tb_hist.dropna(subset=["Full-flowering date (DOY)"])
    tb_hist["country"] = "Japan"
    tb_hist = tb_hist.rename(
        columns={"AD": "year", "Full-flowering date": "full_flowering_date_raw"},
    )
    tb_hist = tb_hist.drop(
        columns=["Source code", "Data type code", "Reference Name"],
    )

    # ── Clean recent data ────────────────────────────────────────────
    tb_recent = tb_recent.rename(
        columns={"Full-flowering date": "full_flowering_date_raw"},
    )

    # ── Combine ──────────────────────────────────────────────────────
    tb = pr.concat([tb_hist, tb_recent], ignore_index=True, short_name="cherry_blossom")

    # ── Convert MDD flowering date → day of year ─────────────────────
    year_zpad = tb["year"].astype(str).str.zfill(4)
    date_raw = tb["full_flowering_date_raw"].astype(float).astype("Int64").astype(str).str.zfill(4)
    date_combine = year_zpad + date_raw
    tb["full_flowering_date"] = date_combine.apply(lambda x: int(datetime.strptime(x, "%Y%m%d").strftime("%j")))
    tb = tb.drop(columns=["Full-flowering date (DOY)", "full_flowering_date_raw"])

    # ── 20-year rolling average ──────────────────────────────────────
    # Origins propagate automatically through .rolling().mean()
    tb = tb.sort_values("year")
    tb["average_20_years"] = tb["full_flowering_date"].rolling(20, min_periods=5).mean()

    # ── Final columns ────────────────────────────────────────────────
    tb["year"] = tb["year"].astype(int)
    tb = tb[["country", "year", "full_flowering_date", "average_20_years"]]

    return export(tb)


# ── Actions ──────────────────────────────────────────────────────────


@Action
def export_csv(cherry_blossom: Dataset):
    """Export cherry blossom data to CSV for quick inspection."""
    df = cherry_blossom.load()
    out = "/tmp/cherry_blossom.csv"
    df.to_csv(out, index=False)
    print(f"  wrote {out} ({len(df)} rows)")
