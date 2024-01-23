import os
from pathlib import Path

import click
import numpy as np
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    """
    Main function to download, process and upload the dataset.
    """
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/urban_agglomerations_largest_cities.csv")
    common_path = "https://population.un.org/wup/Download/Files/"

    # IDs of the files to be downloaded -  visit https://population.un.org/wup/Download/ (Urban and Rural Populations)
    # Each dictionary in the list contains the file name and a description of the data it contains.

    file_details = [
        {
            "file_name": "WUP2018-F11b-30_Largest_Cities_in_2018_by_time.xls",
            "description": "Time Series of the Population of the 30 Largest Urban Agglomerations in 2018 Ranked by Population Size",
        },
        {
            "file_name": "WUP2018-F13-Capital_Cities.xls",
            "description": "Population of Capital Cities in 2018 (thousands)",
        },
    ]

    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()
    # Fetch data from the website and store in a list of DataFrames
    for i, file in enumerate(file_details):
        file_path = os.path.join(common_path, file["file_name"])
        df_add = pd.read_excel(file_path)

        # Find the header row
        header_row_index = None
        for row_idx in range(len(df_add)):
            row_values = df_add.iloc[row_idx]
            if "Index" in row_values.values:
                header_row_index = row_idx + 1
                break

        # If header row is found, re-read the file with correct header
        if header_row_index is not None:
            df_add = pd.read_excel(file_path, skiprows=header_row_index)

        # Exclude specified columns from the dataframe if they exist
        columns_to_exclude = [
            "Index",
            "Note",
            "Country\ncode",
            "Country code",
            "Country Code",
            "City code",
            "Latitude",
            "Longitude",
            "Capital Type",
        ]

        # Create a list of columns to keep
        columns_to_keep = [col for col in df_add.columns if col not in columns_to_exclude]
        df_add = df_add[columns_to_keep]

        if "Population (millions)" in df_add.columns:
            df_add = df_add.rename(
                columns={
                    "Population (millions)": file["description"],
                    "Country or Area": "country",
                    "Urban Agglomeration": "urban_agglomeration",
                    "Year": "year",
                    "Rank\nOrder": "rank_order",
                }
            )

            df_add = df_add.rename(columns={"Population (thousands)": file["description"]})

        if file["description"] == "Population of Capital Cities in 2018 (thousands)":
            df_add = df_add.rename(columns={"Capital City": "urban_agglomeration", "Country or area": "country"})
            df_add["year"] = 2018
            df_add["rank_order"] = "Capital"
        df_add["rank_order"] = df_add["rank_order"].astype(str)
        # If this is the first file, assign the melted DataFrame to merged_df
        if merged_df.empty:
            merged_df = df_add
        else:
            # Otherwise, merge the melted DataFrame with the existing merged_df
            merged_df = pd.merge(
                merged_df,
                df_add,
                on=["year", "rank_order", "country", "urban_agglomeration"],
                how="outer",
            )

    # Save the final DataFrame to a file
    df_to_file(merged_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
