"""Load a snapshot and create a meadow dataset."""

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
    log.info("women_business_law_additional.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("women_business_law_additional.xlsx")

    # Load data from snapshot, for the sheet 2020, 2019 and 2017
    df_2020 = pd.read_excel(snap.path, sheet_name="2020")
    df_2019 = pd.read_excel(snap.path, sheet_name="2019")
    df_2017 = pd.read_excel(snap.path, sheet_name="2017")

    # Concatenate the dataframes
    df = pd.concat([df_2020, df_2019, df_2017], ignore_index=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("women_business_law_additional.end")
