"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Mapping long dimension names to short names.
INDICATOR_MAPPING = {
    "N- Deaths averted by ART Male+Female": "estimate",
    "N- Deaths averted by ART Male+Female; Lower bound": "lower estimate",
    "N- Deaths averted by ART Male+Female; Upper bound": "upper estimate",
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
    snap = cast(Snapshot, paths.load_dependency("unaids_deaths_averted_art.xlsx"))

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="ResultGrid", header=1)

    #
    # Process data.
    #
    log.info("unaids_deaths_averted_art: handle NaNs")
    tb = handle_nans(tb)

    # Rename columns
    log.info("unaids_deaths_averted_art: rename columns and keep relevant ones")
    # Rename columns & keep relevant columns
    tb = tb.rename(columns=INDICATOR_MAPPING | INDEX_MAPPING)[COLUMNS]

    # Format dataframe (wide to long format)
    log.info("unaids_deaths_averted_art: format table with indices and values (wide to long format)")
    tb = tb.melt(id_vars=["country", "year"], var_name="subgroup_description", value_name="deaths_averted_art")

    # Replace dots
    log.info("unaids_deaths_averted_art: replace '...' with NaNs and assign float type")
    tb["deaths_averted_art"] = tb["deaths_averted_art"].replace("...", np.nan).astype(float)

    # Strip empty characters from country names
    log.info("unaids_deaths_averted_art: strip empty characters from country names")
    tb["country"] = tb["country"].str.strip()

    # Create a new table and ensure all columns are snake-case.
    log.info("unaids_deaths_averted_art: format table")
    tb = tb.underscore()
    tb.metadata.short_name = paths.short_name
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
