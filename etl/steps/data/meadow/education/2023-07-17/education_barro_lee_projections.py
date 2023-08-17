"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("education_barro_lee_projections.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #

    # Define a dictionary for renaming columns to have more informative names.
    COLUMNS_RENAME = {
        "BLcode": "Barro-Lee Country Code",
        "WBcode": "World Bank Country Code",
        "region_code": "region",
        "country": "Country",
        "year": "Year",
        "sex": "Sex",
        "agefrom": "Starting Age",
        "ageto": "Finishing Age",
        "lu": "Percentage of no education",
        "lp": "Percentage of primary education",
        "lpc": "Percentage of complete primary education attained",
        "ls": "Percentage of secondary education",
        "lsc": "Percentage of complete secondary education attained",
        "lh": "Percentage of tertiary education",
        "lhc": "Percentage of complete tertiary education attained",
        "yr_sch": "Average years of education",
        "yr_sch_pri": "Average years of primary education",
        "yr_sch_sec": "Average years of secondary education",
        "yr_sch_ter": "Average years of tertiary education",
        "pop": "Population (thousands)",
    }
    # Rename columns in the DataFrame.
    df = df.rename(columns=COLUMNS_RENAME)

    df["age_group"] = df["Starting Age"].astype(str) + "-" + df["Finishing Age"].astype(str)

    # Simple sanity check to see that the values in "Starting Age" and "Finishing Age" are as expected
    starting_ages_expected = {64, 25, 24, 15}
    ages_found = set(df["Starting Age"].append(df["Finishing Age"]))
    ages_unexpected = ages_found - starting_ages_expected
    # Ensure that there are no unexpected ages
    assert not ages_unexpected, f"Unexpected ages in column 'Starting Age': {ages_unexpected}!"

    df.drop(["Starting Age", "Finishing Age"], axis=1, inplace=True)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["country", "year", "sex", "age_group"])

    # Drop unnecessary columns
    tb.drop(["barro_lee_country_code", "world_bank_country_code", "region"], axis=1, inplace=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
