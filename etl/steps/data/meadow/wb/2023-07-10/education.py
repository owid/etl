from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Create a PathFinder instance for the current file
paths = PathFinder(__file__)


def load_and_select_data(snap: Snapshot) -> pd.DataFrame:
    """
    Loads a CSV file from a snapshot and selects data up to the first row containing all NaN values.

    Parameters:
    snap: Snapshot - The snapshot from which the CSV file is to be loaded

    Returns:
    df: DataFrame - The loaded and selected data
    """

    df = pd.read_csv(snap.path, low_memory=False, encoding="latin1")
    first_nan_row_index = df.isnull().all(axis=1).idxmax()
    df = df.iloc[:first_nan_row_index]
    return df


def run(dest_dir: str) -> None:
    """
    Main function to load, process and save all World Bank Education datasets.

    """

    # Define a list of snapshots to be loaded
    snaps = [
        "education_learning_outcomes.csv",
        "education_pre_primary.csv",
        "education_primary.csv",
        "education_secondary.csv",
        "education_tertiary.csv",
        "education_literacy.csv",
        "education_expenditure_and_teachers.csv",
    ]

    # Load and process data from each snapshot
    df_list = []
    for snap in snaps:
        print(snap)
        snapshot = cast(Snapshot, paths.load_dependency(snap))
        df = load_and_select_data(snapshot)
        # Create indicator group colum excluding .csv at the end
        df["Indicator Group"] = snap[:-4]
        print(df["Series"].unique())
        df_list.append(df)

    # Concatenate all processed dataframes
    df = pd.concat(df_list)

    # Perform further processing on the concatenated dataframe
    df.replace("..", np.nan, inplace=True)
    # Drop unnecessary columns that have the same infomariton as Series and Country but in a different format
    df.drop(["Country Code", "Series Code"], axis=1, inplace=True)

    # Clean up year columns (original columns are in the format xxxx [YRxxxx])
    df.columns = df.columns.to_series().apply(
        lambda x: x.split(" ")[0] if x not in ["Country Name", "Series", "Indicator Group"] else x
    )

    # Melt years into a single column
    df_melted = pd.melt(df, id_vars=["Country Name", "Series", "Indicator Group"], var_name="Year", value_name="Value")
    df_melted["Value"] = df_melted["Value"].astype(float)
    df_melted.rename(columns={"Country Name": "country", "Series": "indicator_name"}, inplace=True)

    tb = Table(df_melted, short_name=paths.short_name, underscore=True)

    tb.set_index(["country", "year", "indicator_name"], inplace=True)

    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save the dataset
    ds_meadow.save()
