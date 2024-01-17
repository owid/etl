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
    """
    Main function to download, process and upload the dataset.
    """
    # Create a new snapshot.
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/urban_agglomerations_size_class.csv")
    common_path = "https://population.un.org/wup/Download/Files/"

    # IDs of the files to be downloaded -  visit https://population.un.org/wup/Download/ (Urban and Rural Populations)
    # Each dictionary in the list contains the file name and a description of the data it contains.

    file_details = [
        {
            "file_name": "WUP2018-F17a-City_Size_Class.xls",
            "description": "Urban Population, Number of Cities, and Percentage of Urban Population by Size Class of Urban Settlement, region, subregion, and country, 1950-2035",
        },
        {
            "file_name": "WUP2018-F17b-City_Size_Class-Number.xls",
            "description": "Number of Cities Classified by Size Class of Urban Settlement, region, subregion, and country, 1950-2035",
        },
        {
            "file_name": "WUP2018-F17c-City_Size_Class-Percentage.xls",
            "description": "Percentage of Urban Population in Cities Classified by Size Class of Urban Settlement, region, subregion, and country, 1950-2035",
        },
        {
            "file_name": "WUP2018-F17d-City_Size_Class-Population.xls",
            "description": "Population in Cities Classified by Size Class of Urban Settlement, region, subregion, and country, 1950-2035 (thousands)",
        },
    ]

    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()
    # Fetch data from the website and store in a list of DataFrames
    for i, file in enumerate(file_details):
        file_path = common_path + file["file_name"]
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
        columns_to_exclude = ["Index", "Note", "Country Code", "Size class code"]

        # Create a list of columns to keep
        columns_to_keep = [col for col in df_add.columns if col not in columns_to_exclude]
        df_add = df_add[columns_to_keep]

        # Melt the DataFrame to transform it so that columns other than 'Region, subregion, country or area' become one column
        df_melted = df_add.melt(
            id_vars=["Region, subregion, country or area *", "Size class of urban settlement", "Type of data"],
            var_name="year",
            value_name=file["description"],
        )
        # If this is the first file, assign the melted DataFrame to merged_df
        if merged_df.empty:
            merged_df = df_melted
        else:
            # Otherwise, merge the melted DataFrame with the existing merged_df
            merged_df = pd.merge(
                merged_df,
                df_melted,
                on=["Region, subregion, country or area *", "Size class of urban settlement", "year", "Type of data"],
                how="outer",
            )

    # Save the final DataFrame to a file
    df_to_file(merged_df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
