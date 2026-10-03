"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("education_barro_lee_projections.csv")
    # Load data from snapshot.
    tb = snap.read_csv()

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
    tb = tb.rename(columns=COLUMNS_RENAME)

    tb["age_group"] = tb["Starting Age"].astype(str) + "-" + tb["Finishing Age"].astype(str)

    # Simple sanity check to see that the values in "Starting Age" and "Finishing Age" are as expected
    starting_ages_expected = {64, 25, 24, 15}
    # Assuming tb["Starting Age"] and tb["Finishing Age"] are pandas Series
    ages_combined = pd.concat([tb["Starting Age"], tb["Finishing Age"]])
    ages_found = set(ages_combined)
    ages_unexpected = ages_found - starting_ages_expected
    # Ensure that there are no unexpected ages
    assert not ages_unexpected, f"Unexpected ages in column 'Starting Age': {ages_unexpected}!"

    tb = tb.drop(["Starting Age", "Finishing Age"], axis=1)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "sex", "age_group"], verify_integrity=True).sort_index()

    # Drop unnecessary columns
    tb = tb.drop(["barro_lee_country_code", "world_bank_country_code", "region"], axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
