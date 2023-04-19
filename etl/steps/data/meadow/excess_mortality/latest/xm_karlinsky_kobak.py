"""Load a snapshot and create a meadow dataset.

In this step we perform sanity checks on the expected input fields and the values that they take."""
from pathlib import Path
from typing import List, Union

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Column names
COLUMN_NAMES = [
    "country",
    "year",
    "time",
    "deaths",
]
COLUMN_NAMES_AGES = [
    "country",
    "year",
    "age",
    "sex",
    "time",
    "deaths",
]


def run(dest_dir: str) -> None:
    log.info("xm_karlinsky_kobak.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_all: Snapshot = paths.load_dependency("xm_karlinsky_kobak.csv")
    snap_ages: Snapshot = paths.load_dependency("xm_karlinsky_kobak_ages.csv")

    # Load data from snapshot.
    df_all = load_dataframe(snap_all.path, column_names=COLUMN_NAMES)
    df_ages = load_dataframe(snap_ages.path, column_names=COLUMN_NAMES_AGES)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb_all = Table(df_all, short_name=paths.short_name, underscore=True)
    tb_ages = Table(df_ages, short_name=f"{paths.short_name}_by_age", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap_all.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb_all)
    ds_meadow.add(tb_ages)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("xm_karlinsky_kobak.end")


def load_dataframe(path: Union[Path, str], column_names: List[str]) -> pd.DataFrame:
    """Load the data from the latest version of the dataset."""
    df = pd.read_csv(path, names=column_names)
    # Check columns
    assert (
        df.reset_index().shape[1] == len(column_names) + 1
    ), "Check columns in source! There seems to be more (or less) columns."
    return df
