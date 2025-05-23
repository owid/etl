"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Mapping long dimension names to short names.
DIMENSION_MAPPING = {
    "N- Children prevalence (0-14) (Percent) Male+Female": "0-14 estimate",
    "N- Children prevalence (0-14) (Percent) Male+Female; Lower bound": "0-14 lower estimate",
    "N- Children prevalence (0-14) (Percent) Male+Female; Upper bound": "0-14 upper estimate",
}
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("unaids_hiv_children.xlsx"))

    # Load data from snapshot.
    df = pd.read_excel(snap.path, sheet_name="ResultGrid (5)")

    #
    # Process data.
    #
    log.info("unaids_hiv_children: handle NaNs")
    df = handle_nans(df)

    # Rename columns
    log.info("unaids_hiv_children: rename columns")
    df = df.rename(
        columns={
            "Unnamed: 0": "country",
            "Unnamed: 1": "subgroup_description",
        }
    )
    # Map long dimension names to short names.
    log.info("unaids_hiv_children: set short dimension names")
    df["subgroup_description"] = df["subgroup_description"].map(DIMENSION_MAPPING)

    # Pivot years
    log.info("unaids_hiv_children: melt dataframe (wide -> long format)")
    df = df.melt(id_vars=["country", "subgroup_description"], var_name="year", value_name="hiv_prevalence")

    # Replace dots
    log.info("unaids_hiv_children: replace '...' with NaNs")
    df["hiv_prevalence"] = df["hiv_prevalence"].replace("...", np.nan)

    # Strip empty characters from country names
    log.info("unaids_hiv_children: strip empty characters from country names")
    df["country"] = df["country"].str.strip()

    # Create a new table and ensure all columns are snake-case.
    log.info("unaids_hiv_children: create table")
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["country", "year", "subgroup_description"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()


def handle_nans(df: pd.DataFrame) -> pd.DataFrame:
    """Remove NaNs, and run sanity checks."""
    # Drop all-NaN rows
    df = df.dropna(how="all")
    # Check years
    assert (
        df[df["Unnamed: 1"].isna()].filter(regex=r"\d{4}").nunique() == 1
    ).all(), "Same years used for all countries (same as in header's)."
    # Remove intermediate year-rows
    df = df.dropna(subset=["Unnamed: 0"])

    return df
