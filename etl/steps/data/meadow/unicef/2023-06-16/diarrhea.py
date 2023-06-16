"""Load a snapshot and create a meadow dataset."""

from typing import cast

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
    log.info("diarrhea.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("diarrhea.xlsx"))

    sheets = ["DIARCARE", "ORS", "ORTCF", "ORSZINC", "ZINC"]
    # Load data from snapshot.
    all_sheets_df = pd.DataFrame()
    for sheet in sheets:
        df = pd.read_excel(snap.path, sheet_name=sheet)
        df["indicator"] = sheet
        all_sheets_df = pd.concat([all_sheets_df, df])

    all_sheets_df = all_sheets_df[["Countries and areas", "Year", "National", "indicator"]].reset_index(drop=True)
    all_sheets_df = all_sheets_df.rename(
        columns={"Countries and areas": "country", "Year": "year", "National": "value"}
    )
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(all_sheets_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("diarrhea.end")
