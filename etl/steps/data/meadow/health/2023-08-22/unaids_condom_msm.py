"""Load a snapshot and create a meadow dataset."""

from typing import cast

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
    snap = cast(Snapshot, paths.load_dependency("unaids_condom_msm.xlsx"))

    # Load data from snapshot.
    df = pd.read_excel(snap.path, sheet_name="Condom use among MSM", header=1)

    #
    # Process data.
    #
    log.info("unaids_condom_msm: handle NaNs")
    df = handle_nans(df)

    # Keep relevant columns
    log.info("unaids_condom_msm: keep relevant columns")
    columns = list(df.filter(regex=r"20\d\d$").columns)
    df = df[["Country"] + columns]

    # Unpivot columns (wide -> long format)
    log.info("unaids_condom_msm: unpivot columns (wide -> long format)")
    df = df.melt(id_vars=["Country"], var_name="year", value_name="msm_condom_use")

    # Rename columns
    log.info("unaids_condom_msm: rename columns")
    df = df.rename(
        columns={
            "Country": "country",
        }
    )
    # Map long dimension names to short names.
    log.info("unaids_condom_msm: set short dimension names")
    df["subgroup_description"] = "All ages"

    # Replace dots
    log.info("unaids_condom_msm: replace '...' with NaNs")
    df["msm_condom_use"] = df["msm_condom_use"].astype(float)

    # Drop NaNs
    log.info("unaids_condom_msm: drop NaNs")
    df = df.dropna(subset=["msm_condom_use"])

    # Strip empty characters from country names
    log.info("unaids_condom_msm: strip empty characters from country names")
    df["country"] = df["country"].str.strip()

    # Create a new table and ensure all columns are snake-case.
    log.info("unaids_condom_msm: create table")
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
