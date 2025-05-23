"""Load a snapshot and create a meadow dataset."""

import re
from pathlib import Path
from typing import List, Union

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ozone_hole_area.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_1: Snapshot = paths.load_dependency("ozone_hole_area_p1.txt")
    snap_2: Snapshot = paths.load_dependency("ozone_hole_area_p2.txt")

    # Load data from snapshot.
    df = build_df(snap_1, snap_2)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap_1.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("ozone_hole_area.end")


def build_df_p1(snap: Snapshot) -> pd.DataFrame:
    """Read raw data from source 1 and build a dataframe."""
    log.info("ozone_hole_area: loading data from source 1")
    # Read data
    text = _read_txt_without_comments(snap.path)
    # Sanity checks
    assert re.match(r"\s+Ozone Hole Area\s+Minimum Ozone\n", text[0]), "Data file header might have changed."
    assert re.match(r"\s+Date\s+Value\s+Date\s+Value\n", text[1]), "Data file header might have changed."
    assert re.match(
        r"Year\s+\(YYMM\)\s+\(mil km2\)\s+\(YYMM\)\s+\(DU\)\n", text[2]
    ), "Data file header might have changed."
    assert re.match(r"\-{4}\s+\-{6}\s+\-{9}\s+\-{6}\s+\-{5}\n", text[3]), "Data file header might have changed."
    assert text[4].startswith("1979"), "First expected reported year should be 1979"
    # Get only data values
    text = text[4:]
    text = [re.split(r"\s+", t.strip()) for t in text]
    # Build dataframe
    df = pd.DataFrame.from_records(
        text,
        columns=[
            "year",
            "max_hole_area_date",
            "max_hole_area",
            "min_hole_concentration_date",
            "min_hole_concentration",
        ],
    )
    # Get dates in date format
    df["max_hole_area_date"] = pd.to_datetime(df["year"] + df["max_hole_area_date"], format="%Y%m%d")
    df["min_hole_concentration_date"] = pd.to_datetime(df["year"] + df["min_hole_concentration_date"], format="%Y%m%d")
    return df


def build_df_p2(snap: Snapshot) -> pd.DataFrame:
    """Read raw data from source 2 and build a dataframe."""
    log.info("ozone_hole_area: loading data from source 2")
    # Read data
    text = _read_txt_without_comments(snap.path)
    # Sanity checks
    assert re.match(r"\s+O3 Hole Area\s+Minimum Ozone\n", text[0]), "Data file header might have changed."
    assert re.match(r"Year\s+\(mil km2\)\s+\(DU\)\n", text[1]), "Data file header might have changed."
    assert re.match(r"\-{4}\s+\-{12}\s+\-{13}\n", text[2]), "Data file header might have changed."
    assert text[3].startswith("1979"), "First expected reported year should be 1979"
    # Get only data values
    text = text[3:]
    text = [re.split(r"\s+", t.strip()) for t in text]
    # Build dataframe
    df = pd.DataFrame.from_records(text, columns=["year", "mean_hole_area", "mean_hole_concentration"])
    return df


def build_df(snap_1: Snapshot, snap_2: Snapshot) -> pd.DataFrame:
    """Build a dataframe from the two sources."""
    log.info("ozone_hole_area: merging dataframes")
    # Load two sources
    df1 = build_df_p1(snap_1)
    df2 = build_df_p2(snap_2)
    # Merge
    df = df1.merge(df2, on="year")
    # Check dimensions
    assert len(df) == len(df1) == len(df2), "Some rows went missing wen merged was done!"
    # Set dtypes
    df = df.astype(
        {
            "year": "uint",
            "max_hole_area": "float",
            "min_hole_concentration": "float",
            "mean_hole_area": "float",
            "mean_hole_concentration": "float",
        }
    )
    # Scale based on units
    df["max_hole_area"] *= 1e6
    df["mean_hole_area"] *= 1e6
    return df


def _read_txt_without_comments(path: Union[Path, str]) -> List[str]:
    """Read the text in a file wihout the commented lines.

    A commented line is such that starts with a '#'."""
    with open(path, "r") as f:
        text = [line for line in f.readlines() if not line.startswith("#")]
    return text
