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


def combine_data_from_sheets(data: pd.ExcelFile) -> pd.DataFrame:
    # Select sheet names to use.
    sheet_names = [sheet for sheet in data.sheet_names if "_intensification_by_co" in sheet]

    # Initialize an empty table that will contain all relevant data.
    df = pd.DataFrame({"country": [], "year": []})
    for sheet_name in sheet_names:
        # Item name for current sheet (e.g. "barley").
        item = sheet_name.split("_")[0]
        # Columns to select from each sheet, and how to rename them.
        renaming = {
            "country": "country",
            "attainable yield (t/ha - avg across area of interest)": f"{item}_attainable_yield",
        }

        # Parse data for current sheet.
        df_i = data.parse(sheet_name, skiprows=1)

        # Select and rename required columns.
        df_i = df_i[list(renaming)].rename(columns=renaming, errors="raise")

        # Assign year.
        df_i["year"] = 2000

        # Merge with the combined table.
        df = pd.merge(df, df_i, on=["country", "year"], how="outer")

    return df


def run(dest_dir: str) -> None:
    log.info("mueller_et_al_2012.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("mueller_et_al_2012.xls"))

    # Load data from snapshot.
    data = pd.ExcelFile(snap.path)

    #
    # Process data.
    #
    # Parse and combine data from all sheets.
    df = combine_data_from_sheets(data=data)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("mueller_et_al_2012.end")
