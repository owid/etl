"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
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
    snap = cast(Snapshot, paths.load_dependency("worldbank_pisa.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path, low_memory=True, encoding="latin1")

    # Replace ".." entries with NaN
    df.replace("..", np.nan, inplace=True)

    # Drop unnecessary columns (Country Name and Series Name have the same information)
    df.drop(["Country Code", "Series Code"], axis=1, inplace=True)

    # Find the index of the first row with all NaN values
    first_nan_row_index = df.isnull().all(axis=1).idxmax()

    # Slice the DataFrame up to and including the identified index
    df = df.iloc[:first_nan_row_index]

    # Clean up the column names
    df.columns = df.columns.to_series().apply(lambda x: x.split(" ")[0] if x not in ["Country Name", "Series"] else x)

    # Melt the DataFrame to create a single "Year" column
    df_melted = pd.melt(df, id_vars=["Country Name", "Series"], var_name="Year", value_name="Value")

    # Convert the 'Value' column to float
    df_melted["Value"] = df_melted["Value"].astype(float)

    # Pivot the DataFrame to create a column for each unique value in "Series Name"
    df_pivoted = df_melted.pivot(index=["Country Name", "Year"], columns="Series", values="Value")

    # Reset the index to flatten the DataFrame
    df_pivoted = df_pivoted.reset_index()

    df_pivoted.rename(columns={"Country Name": "country"}, inplace=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_pivoted, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "year"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
