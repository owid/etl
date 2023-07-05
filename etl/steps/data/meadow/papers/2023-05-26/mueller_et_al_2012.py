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
    # Sheet names end in "by_country" for individual countries data, and "by_region" for aggregate regions.
    # However, sometimes the name is not complete, and it ends in "by_re", or "by_countr".
    # But after inspection, the shortest sheet name for region data ends in "by_re".

    # Initialize an empty table that will contain all relevant data.
    df_countries = pd.DataFrame({"country": [], "year": []})
    df_regions = pd.DataFrame({"country": [], "year": []})
    for sheet_name in data.sheet_names:
        # Item name for current sheet (e.g. "barley").
        item = sheet_name.split("_")[0]

        # Parse data for current sheet.
        df_i = data.parse(sheet_name, skiprows=1)

        if "by_re" in sheet_name:
            # Columns to select from each sheet, and how to rename them.
            renaming = {
                "region": "country",
                "attainable yield (t/ha - avg across area of interest)": f"{item}_attainable_yield",
            }
            # Select and rename required columns, and add a year column
            df_i = df_i[list(renaming)].rename(columns=renaming, errors="raise").assign(**{"year": 2000})

            # Merge with the combined table.
            df_countries = pd.merge(df_countries, df_i, on=["country", "year"], how="outer")
        else:
            # Columns to select from each sheet, and how to rename them.
            renaming = {
                "country": "country",
                "attainable yield (t/ha - avg across area of interest)": f"{item}_attainable_yield",
            }

            # Select and rename required columns, and add a year column
            df_i = df_i[list(renaming)].rename(columns=renaming, errors="raise").assign(**{"year": 2000})

            # Merge with the combined table.
            df_regions = pd.merge(df_regions, df_i, on=["country", "year"], how="outer")

    # Concatenate data for regions and for countries.
    df = pd.concat([df_countries, df_regions], ignore_index=True)

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
