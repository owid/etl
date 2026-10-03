"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def combine_data_from_sheets(data: pd.ExcelFile) -> Table:
    # Sheet names end in "by_country" for individual countries data, and "by_region" for aggregate regions.
    # However, sometimes the name is not complete, and it ends in "by_re", or "by_countr".
    # But after inspection, the shortest sheet name for region data ends in "by_re".

    # Initialize an empty table that will contain all relevant data.
    tb_countries = Table({"country": [], "year": []})
    tb_regions = Table({"country": [], "year": []})
    for sheet_name in data.sheet_names:
        # Item name for current sheet (e.g. "barley").
        item = sheet_name.split("_")[0]

        # Parse data for current sheet.
        tb_i = data.parse(sheet_name, skiprows=1)

        if "by_re" in sheet_name:
            # Columns to select from each sheet, and how to rename them.
            renaming = {
                "region": "country",
                "attainable yield (t/ha - avg across area of interest)": f"{item}_attainable_yield",
            }
            # Select and rename required columns, and add a year column
            tb_i = tb_i[list(renaming)].rename(columns=renaming, errors="raise").assign(**{"year": 2000})

            # Merge with the combined table.
            tb_countries = tb_countries.merge(tb_i, on=["country", "year"], how="outer")
        else:
            # Columns to select from each sheet, and how to rename them.
            renaming = {
                "country": "country",
                "attainable yield (t/ha - avg across area of interest)": f"{item}_attainable_yield",
            }

            # Select and rename required columns, and add a year column
            tb_i = tb_i[list(renaming)].rename(columns=renaming, errors="raise").assign(**{"year": 2000})

            # Merge with the combined table.
            tb_regions = tb_regions.merge(tb_i, on=["country", "year"], how="outer")

    # Concatenate data for regions and for countries.
    tb = pr.concat([tb_countries, tb_regions], ignore_index=True, short_name=paths.short_name)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("mueller_et_al_2012.xls")
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Parse and combine data from all sheets.
    tb = combine_data_from_sheets(data=data)

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    # Set an appropriate name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
