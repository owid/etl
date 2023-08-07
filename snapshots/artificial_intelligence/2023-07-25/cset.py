"""Script to create a snapshot of dataset 'Country Activity Tracker: Artificial Intelligence (Center for Security and Emerging Technology, 2023)'."""

from pathlib import Path
from typing import List

import click
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Define the common path where the CSV files are located
COMMON_PATH = "/Users/veronikasamborska/Downloads/owid_cat_data_20230731/"


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    """
    CSET dataset was sent to us directly and researchers asked us not to share the raw data.
    This function that manages the process of reading, cleaning, and combining the data from multiple CSV files on articles, patents and investment in AI
    and stores them into one csv file.
    """

    # Create a new snapshot for storing the combined data
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/cset.csv")

    # Define the mapping between field names and their corresponding CSV files
    files = {
        "companies": ["companies_yearly_disclosed.csv", "companies_yearly_estimated.csv", "companies_summary.csv"],
        "patents": ["patents_yearly_applications.csv", "patents_yearly_granted.csv", "patents_summary.csv"],
        "articles": [
            "publications_yearly_articles.csv",
            "publications_yearly_citations.csv",
            "publications_summary.csv",
        ],
    }

    # Initialize an empty list to store all dataframes (realated to companies, patents and articles)
    all_dfs = []
    for field, file_ids in files.items():
        # For each field, read and clean the data from the CSV files
        all_dfs.append(read_and_clean_data(file_ids, COMMON_PATH, field))

    # Merge all dataframes on the 'year', 'country', and 'field' columns
    final_df = all_dfs[0]
    for df in all_dfs[1:]:
        final_df = pd.merge(final_df, df, on=["year", "country", "field"], how="outer")

    # Save the resulting dataframe to a single csv file
    df_to_file(final_df, file_path=snap.path)
    # Add the snapshot to DVC
    snap.dvc_add(upload=upload)


def read_and_clean_data(file_ids: List[str], common_path: str, field_name: str) -> pd.DataFrame:
    """
    Reads data from a list of CSV files, cleans it, and merges it into a single DataFrame.

    Args:
        file_ids (List[str]): List of file identifiers (file names) to read from.
        common_path (str): The common directory path where all the files are located.
        field_name (str): The field name which will replace the original 'field' column in the merged DataFrame.

    Returns:
        DataFrame: A DataFrame containing the merged and cleaned data.

    The function performs the following steps for each file in file_ids:
        - Reads the file into a pandas DataFrame.
        - If the file contains estimated investment data, renames the corresponding column.
        - Appends the DataFrame to a list of all DataFrames.

    After that, it merges all the DataFrames in the list into a single DataFrame based on the 'year', 'country', and 'field'
    columns. Finally, it renames the 'field' column in the merged DataFrame to the provided field name and returns the result.
    """

    # Initialize an empty list to store all dataframes
    all_dfs_list = []
    for id in file_ids:
        # Read each CSV file into a pandas dataframe
        df_add = pd.read_csv(common_path + id)
        df_add["field"] = df_add["field"].apply(lambda s: s[0] + s[1:].lower() if isinstance(s, str) else s)

        # If the file contains estimated investment, rename the corresponding column
        if "estimated" in id:
            df_add.rename(columns={"disclosed_investment": "investment_estimated"}, inplace=True)
        if "summary" in id:
            rename_dict = {}
            for col in df_add.columns:
                if col not in ["field", "country"]:
                    rename_dict[col] = col + "_summary"
            # Rename the columns using the rename() function
            df_add.rename(columns=rename_dict, inplace=True)
            df_add["year"] = 2022

        # Add the dataframe to the list
        all_dfs_list.append(df_add)

    # Merge all dataframes on the 'year', 'country', and 'field' columns
    merged_df = all_dfs_list[0]
    for df in all_dfs_list[1:]:
        merged_df = pd.merge(merged_df, df, on=["year", "country", "field"], how="outer")

    # Rename the 'field' column to the provided field name (to store the patent type, article field and type of company as separate columns)
    # merged_df.rename(columns={"field": field_name}, inplace=True)

    # Return the cleaned and merged dataframe
    return merged_df


if __name__ == "__main__":
    main()
