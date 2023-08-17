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
    snap = cast(Snapshot, paths.load_dependency("education_lee_lee.xlsx"))
    # Load data from snapshot.
    df = pd.read_excel(snap.path)
    #
    # Process data.
    #
    # Define a dictionary for renaming columns to have more informative names and making sure they are consistent with the 'projections' dataset columns.
    COLUMNS_RENAME = {
        "No Schooling": "Percentage of no education",
        "Primary, total": "Percentage of primary education",
        "Primary, completed": "Percentage of complete primary education attained",
        "Secondary, total": "Percentage of secondary education",
        "Secondary, completed": "Percentage of complete secondary education attained",
        "Tertiary, total": "Percentage of tertiary education",
        "Tertiary, completed": "Percentage of complete tertiary education attained",
        "Avg. Years of Total Schooling": "Average years of education",
        "Avg. Years of Primary Schooling": "Average years of primary education",
        "Avg. Years of Secondary Schooling": "Average years of secondary education",
        "Avg. Years of Tertiary\n Schooling": "Average years of tertiary education",
        "Population\n(1000s)": "Population (thousands)",
        "Primary": "Primary enrollment rates",
        "Secondary": "Secondary enrollment rates",
        "Tertiary": "Tertiary enrollment rates",
    }
    # Rename columns in the DataFrame.
    df = df.rename(columns=COLUMNS_RENAME)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    tb.set_index(["country", "year", "sex", "age_group"], inplace=True)
    # Drop unnecessary columns
    tb = tb.drop("region", axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
