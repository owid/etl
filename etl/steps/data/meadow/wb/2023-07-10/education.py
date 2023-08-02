import os
import zipfile

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Create a PathFinder instance for the current file
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Main function to load, process and save all World Bank Education datasets.

    """
    snap: Snapshot = paths.load_dependency("education.zip")
    # Replace 'data.zip' with the name of your zip file

    # Step 1: Unzip the file
    with zipfile.ZipFile(snap.path, "r") as zip_ref:
        # Replace 'data.csv' with the name of your CSV file in the zip archive
        csv_file_name = "EdStatsData.csv"
        destination_directory = os.path.dirname(snap.path)
        zip_ref.extract(csv_file_name, destination_directory)

    # Now, use pandas to read the CSV file into a DataFrame
    df = pd.read_csv(os.path.join(destination_directory, csv_file_name))

    df.dropna(axis=1, how="all", inplace=True)

    # Perform further processing on the concatenated dataframe
    df.replace("..", np.nan, inplace=True)
    # Drop unnecessary country code column
    df.drop("Country Code", axis=1, inplace=True)

    # Melt years into a single column
    df_melted = pd.melt(
        df, id_vars=["Country Name", "Indicator Name", "Indicator Code"], var_name="Year", value_name="Value"
    )

    df_melted["Value"] = df_melted["Value"].astype(float)
    df_melted.rename(columns={"Country Name": "country", "Indicator Name": "indicator_name"}, inplace=True)

    tb = Table(df_melted, short_name=paths.short_name, underscore=True)
    tb.set_index(["country", "year", "indicator_name"], inplace=True)

    # Use metadata from the first snapshot, then edit the descriptions in the garden step
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save the dataset
    ds_meadow.save()
