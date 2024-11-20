"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("unwto.xlsx")

    #
    # Process data.
    #

    sheet_names_to_load = [
        " Inbound Tourism-Arrivals",
        "Inbound Tourism-Regions",
        "Inbound Tourism-Purpose",
        "Inbound Tourism-Transport",
        "Inbound Tourism-Accommodation",
        "Inbound Tourism-Expenditure",
        "Domestic Tourism-Trips",
        "Domestic Tourism-Accommodation",
        "Outbound Tourism-Departures",
        "Tourism Industries",
        "Employment",
        "Outbound Tourism-Expenditure",
    ]

    # Year range
    year_range = range(1995, 2023)

    tbs = []
    for sheet_name in sheet_names_to_load:
        tb = snap.read(safe_types=False, sheet_name=sheet_name, header=2)

        # Drop unnecessary columns
        columns_to_drop = ["C.", "S.", "C. & S.", "Units", "Notes", "Series", "Unnamed: 38", "Unnamed: 39"]
        tb = tb.drop(columns=[col for col in columns_to_drop if col in tb.columns])

        # Drop rows and columns with all NaN values
        tb = tb.dropna(how="all", axis=1).dropna(how="all", axis=0)

        # Rename the 'Basic data and indicators' column to 'country'
        tb = tb.rename(columns={"Basic data and indicators": "country"})

        # Melt into a long format
        non_year_cols = [col for col in tb.columns if col not in year_range]
        tb = tb.melt(id_vars=non_year_cols, value_vars=year_range, var_name="year")

        # Fill missing country names with the previous valid value
        tb["country"] = tb["country"].ffill()

        # Drop rows with all NaN values in columns other than 'country', 'year', and 'value'
        tb = tb.dropna(subset=tb.columns.difference(["country", "year", "value"]), how="all")

        # Forward fill specific columns for certain sheets
        if sheet_name in ["Inbound Tourism-Accommodation", "Domestic Tourism-Accommodation"]:
            cols_to_fill = [col for col in tb.columns if col not in ["country", "year", "value", "Unnamed: 6"]]
            tb[cols_to_fill] = tb[cols_to_fill].ffill()

        # Combine relevant columns into 'indicator'
        columns_to_combine = tb.drop(columns=["country", "value", "year"]).fillna("").astype(str)
        tb["indicator"] = columns_to_combine.agg(" ".join, axis=1)

        # Keep only the necessary columns now that indicator column is created
        tb = tb[["country", "year", "value", "indicator"]]

        # Drop rows with missing 'value' and replace '..' with NaN
        tb = tb.dropna(subset=["value"])
        tb["value"] = tb["value"].replace("..", np.nan)

        # Add the sheet name to the 'indicator' column
        tb["indicator"] = sheet_name + "-" + tb["indicator"].astype(str)

        # Set the index to 'country', 'year', and 'indicator' and ensure it's unique (helps to debug if there are issues)
        tb = tb.set_index(["country", "year", "indicator"])
        assert tb.index.is_unique, f"Index is not unique in sheet '{sheet_name}'."
        tb = tb.reset_index()

        tbs.append(tb)

    # Concatenate all the processed Tables
    tb = pr.concat(tbs, axis=0)

    # Convert 'value' to float
    tb["value"] = tb["value"].astype(float)

    # Pivot the Table to have 'indicator' as columns and 'value' as cell values
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
