"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns and their new names
COLUMNS_WITH_VALUES = {
    "Education": "education",
    "Health": "health",
    "Pensions": "pensions",
    "Unemp": "unemployment",
    "Other soc exp.'": "other_social_expenditure",
    "Tot soc exp. \n(with educ)": "total_social_expenditure_with_education",
    "GDP": "gdp",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("social_expenditure_1985.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="OECD 1985 original", header=[0, 1])

    #
    # Process data.
    #
    # Move the first level of columns to the rows
    tb = tb.stack(level=0, future_stack=True).reset_index(drop=False)

    # Rename Unnamed: 0_level_1 to year and level_1 to country
    tb = tb.rename(columns={"Unnamed: 0_level_1": "year", "level_1": "country"})

    # Drop level_0
    tb = tb.drop(columns=["level_0"])

    # Fill missing rows in year with the first non-missing value
    tb["year"] = tb["year"].ffill()

    # Drop the country value Unnamed: 0_level_0
    tb = tb[tb["country"] != "Unnamed: 0_level_0"].reset_index(drop=True)

    # Rename relevant columns
    tb = tb.rename(columns=COLUMNS_WITH_VALUES)

    # Make all the columns with values numeric
    for col in COLUMNS_WITH_VALUES.values():
        tb[col] = tb[col].apply(pd.to_numeric, errors="coerce")

    # Keep only the columns we are interested in
    tb = tb[["country", "year"] + list(COLUMNS_WITH_VALUES.values())]

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
