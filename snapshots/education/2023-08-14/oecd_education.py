"""Script to create a snapshot of dataset."""

import os
from pathlib import Path

import click
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Constants for specific Excel file formats
SKIP_ROWS = {
    "literacy_ed_attainment": 7,
    "average_years_education_countries": 8,
    "ed_attainment_average_years_regions": 9,
}

HEADER_ROW = {
    "literacy_ed_attainment": 1,
    "average_years_education_countries": 0,
    "ed_attainment_average_years_regions": 0,
}


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    """
    Main function to extract, process, and merge historical datasets on educational attainment and literacy and
    then save as the final snapshot.

    Data is paywalled and had to be accessed through a university account through this link - https://www.oecd-ilibrary.org/economics/how-was-life/education-since-1820_9789264214262-9-en
    by clicking on:
      * World development of literacy and attainment of at least basic education, 1820-2010
      *	Population having attained at least basic education by region, 1870-2010
      * Average years of education by region, 1850-2010
      * Average years of education in selected countries, 1850-2000

    The OECD responded that despite being paywalled the data can be reproduced if appropriately cited according to the terms and conditions here https://www.oecd.org/termsandconditions/.

    """
    # Create a new snapshot.
    snap = Snapshot(f"education/{SNAPSHOT_VERSION}/oecd_education.csv")

    # Set the directory where the downloaded files are
    directory = "/Users/veronikasamborska/Downloads/"

    # Excel files with regional data on literacy and educational attainment data
    region_files = [
        "9789264214262-table28-en.xls",
        "9789264214262-table29-en.xls",
    ]
    # Matching variable names in the corresponding excel files
    variable_names = ["population_with_basic_education", "average_years_of_education"]
    dfs_regions = []
    # Extract data from each regional file and store in separate DataFrames
    for variable_name, file_name in zip(variable_names, region_files):
        filepath = os.path.join(directory, file_name)
        df = extract_ed_attainment_average_years_regions(filepath, variable_name)
        df["country_or_region"] = df["country_or_region"].str.replace(r"\s+", " ", regex=True)
        dfs_regions.append(df)
    # Combine the extracted regional DataFrames into one
    dfs_regions = pd.merge(dfs_regions[0], dfs_regions[1], on=["year", "country_or_region"], how="outer")

    # Extract data related to literacy and average years of education
    filepath_literacy = os.path.join(directory, "9789264214262-graph27-en.xls")
    df_literacy = extract_literacy_ed_attainment(filepath_literacy)
    df_literacy["country_or_region"] = df_literacy["country_or_region"].str.replace(r"\s+", " ", regex=True)

    filepath_ed_years = os.path.join(directory, "9789264214262-table30-en.xls")
    df_ed_years = extract_average_years_education_countries(filepath_ed_years)
    df_ed_years["country_or_region"] = df_ed_years["country_or_region"].str.replace(r"\s+", " ", regex=True)

    # Merge the three datasets on year and country/region
    all_dfs = pd.merge(dfs_regions, df_literacy, on=["year", "country_or_region"], how="outer")
    # Merge on average_years_of_education - country and regional data
    all_dfs = pd.merge(
        all_dfs, df_ed_years, on=["year", "country_or_region", "average_years_of_education"], how="outer"
    )

    # Save the merged dataset to the snapshot path
    df_to_file(all_dfs, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def read_excel_data(filepath, format_type):
    """Helper function to read excel data based on format."""
    return pd.read_excel(filepath, skiprows=SKIP_ROWS[format_type], header=HEADER_ROW[format_type])


def extract_literacy_ed_attainment(filepath):
    """
    Extracts and cleans data from the specified Excel file tailored for literacy and educational attainment data.

    Parameters:
    - filepath (str): Absolute path to the Excel file.

    Returns:
    - DataFrame: A cleaned DataFrame with selected columns.
    """
    data_frame = read_excel_data(filepath, "literacy_ed_attainment")
    cleaned_data_frame = data_frame.dropna(axis=1, how="all").iloc[:, 1:5]
    cleaned_data_frame.columns = ["year", "literacy", "best_guess", "educational attainment"]
    cleaned_data_frame = cleaned_data_frame.dropna(axis=0, how="all")
    cleaned_data_frame["country_or_region"] = "World"
    cleaned_data_frame["year"] = cleaned_data_frame["year"].astype("int64")
    return cleaned_data_frame


def extract_average_years_education_countries(filepath):
    """
    Extracts, cleans, and melts data from the specified Excel file tailored for average years of education by country.

    Parameters:
    - filepath (str): Absolute path to the Excel file.

    Returns:
    - DataFrame: A melted DataFrame after cleaning.
    """
    data_frame = read_excel_data(filepath, "average_years_education_countries")
    cleaned_data_frame = data_frame.dropna(axis=1, how="all").reset_index(drop=True).iloc[:19, :]
    melted_data_frame = cleaned_data_frame.melt(
        id_vars=[cleaned_data_frame.columns[0]], var_name="country_or_region", value_name="average_years_of_education"
    )
    melted_data_frame.rename(columns={melted_data_frame.columns[0]: "year"}, inplace=True)
    melted_data_frame["year"] = melted_data_frame["year"].astype("int64")
    return melted_data_frame


def extract_ed_attainment_average_years_regions(filepath, variable_name):
    """
    Extracts, processes, and melts data from the specified Excel file tailored for education attainment average years by region.

    Parameters:
    - filepath (str): Absolute path to the Excel file.
    - variable_name (str): Desired name for the value column in the melted DataFrame.

    Returns:
    - DataFrame: A melted DataFrame after processing.
    """
    regions_data_frame = read_excel_data(filepath, "ed_attainment_average_years_regions")
    regions = pd.read_excel(filepath, skiprows=4, nrows=3, header=None)
    regions_list = regions.iloc[2, 1:].values
    regions_data_frame.columns = ["year"] + list(regions_list)
    regions_data_frame = regions_data_frame[:15]
    melted_regions_data_frame = pd.melt(
        regions_data_frame,
        id_vars="year",
        value_vars=regions_data_frame.columns[1:],
        var_name="country_or_region",
        value_name=variable_name,
    )
    melted_regions_data_frame["year"] = melted_regions_data_frame["year"].astype("int64")
    return melted_regions_data_frame


if __name__ == "__main__":
    main()
