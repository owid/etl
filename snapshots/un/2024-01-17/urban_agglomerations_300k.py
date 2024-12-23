"""
This script is designed to create a snapshot of the 'World Urbanization Prospects Dataset - Urban Agglomerations with 300,000 Inhabitants or more' from the United Nations.
The script downloads four Excel files from the UN website. Each file contains different data related to urban agglomerations with 300,000 inhabitants or more, including the average annual rate of change, the percentage of the urban population, the percentage of the total population, and the annual population.
Each file is processed by cleaning the data, excluding certain columns, and reshaping the data into a format that is easier to analyze.
The data from each file is merged into a single DataFrame. This is done by merging on the country, urban agglomeration, latitude, and year.
"""

import os
from pathlib import Path

import click
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
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/urban_agglomerations_300k.csv")
    common_path = "https://population.un.org/wup/Download/Files/"

    # IDs of the files to be downloaded -  visit https://population.un.org/wup/Download/ (Urban and Rural Populations)
    # Each dictionary in the list contains the file name and a description of the data it contains.
    file_details = [
        {
            "file_name": "WUP2018-F14-Growth_Rate_Cities.xls",
            "description": "Average Annual Rate of Change of Urban Agglomerations with 300,000 Inhabitants or More in 2018, by country, 1950-2035 (percent)",
        },
        {
            "file_name": "WUP2018-F15-Percentage_Urban_in_Cities.xls",
            "description": "Percentage of the Urban Population Residing in Each Urban Agglomeration with 300,000 Inhabitants or More in 2018, by country, 1950-2035",
        },
        {
            "file_name": "WUP2018-F16-Percentage_Total_in_Cities.xls",
            "description": "Percentage of the Total Population Residing in Each Urban Agglomeration with 300,000 Inhabitants or More in 2018, by country, 1950-2035",
        },
        {
            "file_name": "WUP2018-F22-Cities_Over_300K_Annual.xls",
            "description": "Annual Population of Urban Agglomerations with 300,000 Inhabitants or More, by country, 1950-2035 (thousands)",
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
        columns_to_exclude = ["Index", "Note", "Country Code", "City Code", "Longitude"]

        # Create a list of columns to keep
        columns_to_keep = [col for col in df_add.columns if col not in columns_to_exclude]
        df_add = df_add[columns_to_keep]

        # Melt the DataFrame to transform it so that columns other than 'Region, subregion, country or area' become one column
        df_melted = df_add.melt(
            id_vars=["Country or area", "Urban Agglomeration", "Latitude"],
            var_name="year",
            value_name=file["description"],
        )

        df_melted["year"] = df_melted["year"].astype(str)
        # Extract values after the dash in the "year" column (e.g. 1950-1955 becomes 1955 for rate of change data)
        df_melted["year"] = df_melted["year"].str.split("-").str[-1]

        # If this is the first file, assign the melted DataFrame to merged_df
        if merged_df.empty:
            merged_df = df_melted
        else:
            # Otherwise, merge the melted DataFrame with the existing merged_df
            merged_df = pd.merge(
                merged_df,
                df_melted,
                on=["Country or area", "Urban Agglomeration", "Latitude", "year"],
                how="outer",
            )

    # Save the final DataFrame to a file
    df_to_file(merged_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
