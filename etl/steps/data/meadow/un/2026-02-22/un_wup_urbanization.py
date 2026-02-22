"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets to process
SHEETS = {
    "Rural": "rural",
    "Cities": "cities",
    "Towns": "towns",
    "Cities and Towns": "cities_and_towns",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("un_wup_urbanization.xlsx")

    # Process each sheet
    tables = []
    for sheet_name, table_name in SHEETS.items():
        # Load data from snapshot.
        tb = snap.read(sheet_name=sheet_name, header=0)

        # Keep only essential columns
        metadata_cols = ["Location", "LocID", "ISO3_Code", "LocTypeName"]
        year_cols = [col for col in tb.columns if col.isdigit()]

        tb = tb[metadata_cols + year_cols]

        # Filter to only keep countries and areas (exclude regional aggregates)
        # LocTypeName values: Country/Area = 4, others are regional groupings
        tb = tb[tb["LocTypeName"].isin(["Country/Area"])].copy()

        # Reshape from wide to long format
        tb = tb.melt(id_vars=metadata_cols, value_vars=year_cols, var_name="year", value_name="rate_of_change")

        # Convert year to integer
        tb["year"] = tb["year"].astype(int)

        # Rename Location to country
        tb = tb.rename(columns={"Location": "country"})

        # Drop rows with missing values
        tb = tb.dropna(subset=["rate_of_change"])

        # Drop the LocTypeName column as it's no longer needed
        tb = tb.drop(columns=["LocTypeName"])

        #
        # Process data.
        #
        # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
        tb = tb.format(["country", "year"], short_name=table_name)

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
