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
    log.info("wgm_2018: start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("wgm_2018.xlsx")

    # Load data from snapshot.
    log.info("wgm_2018: load data")
    dfs = pd.read_excel(snap.path, sheet_name=None)

    # Data
    df = dfs["Full dataset"]
    # Set dtype for all questions to str
    columns = [col for col in df.columns if col not in ["wgt", "projwt"]]
    df[columns] = df[columns].astype(str)
    # Metadata
    df_metadata = dfs["Data dictionary"]
    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    log.info("wgm_2018: create tables")
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb_meta = Table(df_metadata, short_name=f"{paths.short_name}_metadata", underscore=True)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    log.info("wgm_2018: create dataset")
    ds_meadow = create_dataset(dest_dir, tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wgm_2018: end")
