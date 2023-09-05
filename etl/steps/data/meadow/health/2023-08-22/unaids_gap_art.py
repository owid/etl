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
INDICATOR_MAPPING = {
    "N-2nd 90% - Gap to reaching the target of people receiving ART (90% of PLHIV)": "All ages",
}
# Map column names
INDEX_MAPPING = {
    "Unnamed: 0": "year",
    "Unnamed: 2": "country",
}
# Names of relevant columns
COLUMNS = list(INDEX_MAPPING.values()) + list(INDICATOR_MAPPING.values())

# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("unaids_gap_art.xlsx"))

    # Load data from snapshot.
    df = pd.read_excel(snap.path, sheet_name="ResultGrid (1)", header=1)

    #
    # Process data.
    #
    log.info("unaids_gap_art: handle NaNs")
    df = handle_nans(df)

    # Rename columns
    log.info("unaids_gap_art: rename columns and keep relevant ones")
    # Rename columns & keep relevant columns
    df = df.rename(columns=INDICATOR_MAPPING | INDEX_MAPPING)[COLUMNS]

    # Format dataframe (wide to long format)
    log.info("unaids_gap_art: format table with indices and values (wide to long format)")
    df = df.melt(id_vars=["country", "year"], var_name="subgroup_description", value_name="gap_on_art")

    # Replace dots
    log.info("unaids_gap_art: replace '...' with NaNs and assign float type")
    df["gap_on_art"] = df["gap_on_art"].replace("...", np.nan).astype(float)

    # Drop NaNs
    log.info("unaids_gap_art: drop NaNs")
    df = df.dropna(subset=["gap_on_art"])

    # Strip empty characters from country names
    log.info("unaids_gap_art: strip empty characters from country names")
    df["country"] = df["country"].str.strip()

    # Create a new table and ensure all columns are snake-case.
    log.info("unaids_gap_art: create table")
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

    return df
