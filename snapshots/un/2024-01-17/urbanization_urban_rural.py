"""
This script is designed to create a snapshot of the 'World Urbanization Prospects Dataset - Urban and Rural Population' from the United Nations.
The script downloads several Excel files from the UN website. Each file contains data on urban and rural population growth rates, total growth rates, and annual population numbers for different regions, subregions, and countries from 1950 to 2050. The data from each file is cleaned, reshaped, and merged into a single DataFrame.
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
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/urbanization_urban_rural.csv")
    common_path = "https://population.un.org/wup/Download/Files/"

    # IDs of the files to be downloaded -  visit https://population.un.org/wup/Download/ (Urban and Rural Populations)
    # Each dictionary in the list contains the file name and a description of the data it contains.
    file_details = [
        {
            "file_name": "WUP2018-F06-Urban_Growth_Rate.xls",
            "description": "Average Annual Rate of Change of the Urban Population by region, subregion and country, 1950-2050 (percent)",
        },
        {
            "file_name": "WUP2018-F07-Rural_Growth_Rate.xls",
            "description": "Average Annual Rate of Change of the Rural Population by region, subregion and country, 1950-2050 (percent)",
        },
        {
            "file_name": "WUP2018-F08-Total_Growth_Rate.xls",
            "description": "Average Annual Rate of Change of the Total Population by region, subregion and country, 1950-2050 (percent)",
        },
        {
            "file_name": "WUP2018-F09-Urbanization_Rate.xls",
            "description": "Average Annual Rate of Change of the Percentage Urban by region, subregion and country, 1950-2050 (percent)",
        },
        {
            "file_name": "WUP2018-F10-Rate_Proportion_Rural.xls",
            "description": "Average Annual Rate of Change of the Percentage Rural by region, subregion and country, 1950-2050 (percent)",
        },
        {
            "file_name": "WUP2018-F19-Urban_Population_Annual.xls",
            "description": "Annual Urban Population at Mid-Year by region, subregion and country, 1950-2050 (thousands)",
        },
        {
            "file_name": "WUP2018-F20-Rural_Population_Annual.xls",
            "description": "Annual Rural Population at Mid-Year by region, subregion and country, 1950-2050 (thousands)",
        },
        {
            "file_name": "WUP2018-F21-Proportion_Urban_Annual.xls",
            "description": "Annual Percentage of Population at Mid-Year Residing in Urban Areas by region, subregion and country, 1950-2050",
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
        columns_to_exclude = ["Index", "Note", "Country\ncode"]

        # Create a list of columns to keep
        columns_to_keep = [col for col in df_add.columns if col not in columns_to_exclude]
        df_add = df_add[columns_to_keep]

        # Melt the DataFrame to transform it so that columns other than 'Region, subregion, country or area' become one column
        df_add = df_add.melt(
            id_vars=["Region, subregion, country or area"], var_name="year", value_name=file["description"]
        )
        df_add["year"] = df_add["year"].astype(str)
        # Extract values after the dash in the "year" column (e.g. 1950-1955 becomes 1955 for rate of change data)
        df_add["year"] = df_add["year"].str.split("-").str[-1]

        # If this is the first file, assign the melted DataFrame to merged_df
        if merged_df.empty:
            merged_df = df_add
        else:
            # Otherwise, merge the melted DataFrame with the existing merged_df
            merged_df = pd.merge(merged_df, df_add, on=["Region, subregion, country or area", "year"], how="outer")

    # Save the final DataFrame to a file
    df_to_file(merged_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
