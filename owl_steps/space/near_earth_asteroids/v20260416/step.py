"""
Near-Earth Asteroids — cumulative discovery counts by size category.

Source: NASA Center for Near-Earth Object Studies (CNEOS)
        https://cneos.jpl.nasa.gov/stats/totals.html
"""

import subprocess
import tempfile
from pathlib import Path

import pandas as pd
from owl.catalog import export, load_snapshot
from owl.dataset import Dataset
from owl.snapshot import Snapshot

# ── Helpers ──────────────────────────────────────────────────────────

R2_BUCKET = "owid-snapshots"


def _fetch_from_r2(md5: str, suffix: str) -> Path:
    prefix, rest = md5[:2], md5[2:]
    key = f"{prefix}/{rest}"
    tmp = Path(tempfile.mkdtemp()) / f"snapshot{suffix}"
    subprocess.run(
        ["rclone", "cat", f"r2:{R2_BUCKET}/{key}"],
        stdout=open(tmp, "wb"),
        check=True,
    )
    return tmp


# ── Snapshot ─────────────────────────────────────────────────────────


@Snapshot(version="2026-01-08")
def raw_data():
    """NASA CNEOS cumulative totals of near-Earth asteroids."""
    path = _fetch_from_r2("aae7e75196978dff4eb61598035a5fd3", ".csv")
    return pd.read_csv(path)


# ── Dataset ──────────────────────────────────────────────────────────


@Dataset
def near_earth_asteroids(raw_data: Snapshot):
    """Clean data and compute size-bucket counts."""

    tb = load_snapshot(raw_data, short_name="near_earth_asteroids")

    # Keep only the columns we need
    tb = tb[["Date", "NEA-km", "NEA-140m", "NEA"]]

    # Sort by date descending, extract year
    tb = tb.sort_values("Date", ascending=False)
    tb["year"] = tb["Date"].str[0:4].astype(int)

    # Filter out the current (incomplete) year
    date_published = raw_data.meta.get("origin", {}).get("date_published", "2026")
    current_year = int(str(date_published)[0:4])
    tb = tb[tb["year"] < current_year]

    # Keep only the latest record per year
    tb = tb.drop_duplicates(subset="year")

    # Compute size buckets
    tb["larger_than_1km"] = tb["NEA-km"]
    tb["between_140m_and_1km"] = tb["NEA-140m"] - tb["NEA-km"]
    tb["smaller_than_140m"] = tb["NEA"] - tb["NEA-140m"]

    # Add entity column
    tb["country"] = "World"

    # Final columns
    tb = tb[["country", "year", "larger_than_1km", "between_140m_and_1km", "smaller_than_140m"]]
    tb = tb.reset_index(drop=True)

    return export(tb)
