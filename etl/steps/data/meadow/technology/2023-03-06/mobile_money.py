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
    log.info("mobile_money.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("mobile_money.xlsx")

    # Load data from snapshot.
    df = pd.read_excel(snap.path, sheet_name="Accounts", usecols="B:BK", skiprows=25, nrows=16)

    # Reshape to tidy format and rename cols
    df = df.melt(id_vars="Regions", var_name="year", value_name="active_accounts_90d").rename(
        columns={"Regions": "region"}
    )

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("mobile_money.end")
