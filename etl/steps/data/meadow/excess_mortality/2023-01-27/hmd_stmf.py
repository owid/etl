"""Load a snapshot and create a meadow dataset.

In this step we perform sanity checks on the expected input fields and the values that they take."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers.misc import check_known_columns
from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


COLUMNS_EXPECTED = [
    "CountryCode",
    "Year",
    "Week",
    "Sex",
    "D0_14",
    "D15_64",
    "D65_74",
    "D75_84",
    "D85p",
    "DTotal",
    "R0_14",
    "R15_64",
    "R65_74",
    "R75_84",
    "R85p",
    "RTotal",
    "Split",
    "SplitSex",
    "Forecast",
]


def run(dest_dir: str) -> None:
    log.info("hmd_stmf.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("hmd_stmf.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path, skiprows=1)
    check_known_columns(df, COLUMNS_EXPECTED)
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("hmd_stmf.end")
