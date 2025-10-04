"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_terrorism_index.xlsx")

    # Load data from snapshot - read sheets for years 2024 through 2011.
    tables = []
    for year in range(2024, 2010, -1):  # 2024 down to 2011
        # First, read the sheet to find the header row
        tb_raw = snap.read_excel(sheet_name=str(year), header=None)

        # Find the row that contains the header
        header_row = None
        for idx, row in tb_raw.iterrows():
            # Check if this row contains the expected header columns
            row_str = " ".join([str(cell) for cell in row if pd.notna(cell)])
            if "Country" in row_str and "iso3c" in row_str and "rank" in row_str and "Score" in row_str:
                header_row = idx
                break

        if header_row is not None:
            # Read again with the correct header row
            tb_year = snap.read_excel(sheet_name=str(year), skiprows=header_row)
            columns = ["Country", "Incidents", "Fatalities", "Hostages", "Injuries"]
            tb_year = tb_year[columns]
            tb_year["year"] = year

            tables.append(tb_year)
        else:
            paths.log.info(f"Header row not found in sheet {year}")

    # Combine all yearly tables
    if tables:
        tb = pr.concat(tables, ignore_index=True)
    else:
        raise ValueError("No valid sheets found for years 2024-2011")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
