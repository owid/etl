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
from owid.catalog import Table
from owid.catalog import processing as pr
from owl.catalog import export, load_snapshot
from owl.dataset import Action, Dataset
from owl.grapher import upsert_dataset
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


@Snapshot
def historical_data() -> Path:
    """Historical cherry blossom data (812–2015) from Aono's XLS."""
    return _fetch_from_r2("fc459738bd4a0a73c716755d598c6678", ".xls")


@Snapshot
def recent_data() -> Path:
    """Recent years (2016–2026) from personal communication with Aono."""
    return _fetch_from_r2("ef18617c3feeb2bb3fa5158f31ac9997", ".csv")


# ── Dataset ──────────────────────────────────────────────────────────


@Dataset
def cherry_blossom(historical_data: Snapshot, recent_data: Snapshot) -> tuple[pd.DataFrame, dict]:
    """Combine historical + recent data, convert dates, compute rolling average."""

    tb_hist = load_snapshot(historical_data, skiprows=25)
    tb_recent = load_snapshot(recent_data)

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
    tb = pr.concat([tb_recent, tb_hist], ignore_index=True, short_name="cherry_blossom")

    # ── Convert MDD flowering date → day of year ─────────────────────
    year_zpad = tb["year"].astype(str).str.zfill(4)
    date_raw = tb["full_flowering_date_raw"].astype(float).astype("Int64").astype(str).str.zfill(4)
    date_combine = year_zpad + date_raw
    tb["full_flowering_date"] = date_combine.apply(lambda x: int(datetime.strptime(x, "%Y%m%d").strftime("%j")))
    tb = tb.drop(columns=["Full-flowering date (DOY)", "full_flowering_date_raw"])

    # ── 30-year rolling average ──────────────────────────────────────
    # Add rows for missing years so the rolling window matches the ETL step.
    all_years = Table(pd.DataFrame({"year": range(tb["year"].min(), tb["year"].max() + 1), "country": "Japan"}))
    tb = pr.merge(all_years, tb, on=["year", "country"], how="left").copy_metadata(tb)
    tb["average_last_30_years"] = tb["full_flowering_date"].rolling(30, min_periods=10).mean()

    # ── Final columns ────────────────────────────────────────────────
    tb["year"] = tb["year"].astype(int)
    tb = tb[["country", "year", "full_flowering_date", "average_last_30_years"]]
    tb = tb.dropna(subset=["full_flowering_date", "average_last_30_years"], how="all")

    return export(tb)


# ── Actions ──────────────────────────────────────────────────────────


@Action(kind="grapher")
def upsert_to_grapher(cherry_blossom: Dataset) -> None:
    """Upsert the cherry blossom dataset to Grapher MySQL."""
    dataset_id = upsert_dataset(cherry_blossom)
    print(f"  upserted Grapher dataset {dataset_id}")
