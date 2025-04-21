"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
import zipfile
from typing import List

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cset.zip")
    #
    # Process data.
    #
    # Define the mapping between field names and their corresponding CSV files
    files = {
        "companies": ["companies_yearly_disclosed.csv", "companies_yearly_estimated.csv"],
        "patents": ["patents_yearly_applications.csv", "patents_yearly_granted.csv"],
        "articles": [
            "publications_yearly_articles.csv",
            "publications_yearly_citations.csv",
        ],
    }

    # Initialize an empty list to store all dataframes (related to companies, patents, and articles)
    all_dfs = []

    # Use a temporary directory to extract the ZIP file
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(snap.path, "r") as zip_file:
            zip_file.extractall(temp_dir)  # Extract all files to the temporary directory

        # Process each field and its associated files
        for field, file_ids in files.items():
            all_dfs.append(read_and_clean_data(file_ids, temp_dir, field))

    # Merge all dataframes on the 'year', 'country', and 'field' columns
    final_df = all_dfs[0]
    for df in all_dfs[1:]:
        final_df = pd.merge(final_df, df, on=["year", "country", "field", "type"], how="outer")

    tb = Table(final_df, short_name=paths.short_name, underscore=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "field", "type"])
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_and_clean_data(file_ids: List[str], temp_dir: str, field_name: str) -> pd.DataFrame:
    """
    Reads data from a list of CSV files inside a temporary directory, cleans them, and merges into a single DataFrame.
    """
    all_dfs_list = []
    for file_name in file_ids:
        # Construct the full path to the file in the temporary directory
        file_path = os.path.join(temp_dir, "cat", file_name)

        # Read the CSV file
        df_add = pd.read_csv(file_path)
        if "complete" in df_add.columns:
            # Rename the column 'complete' to 'type'
            df_add = df_add.rename(columns={"complete": "type"})

            # Convert the 'type' column to string and replace values
            df_add["type"] = df_add["type"].astype(str).replace({"True": "estimate", "False": "projection"})
            # Find the lowest year with type == "projection" for each country and field
            projection_rows = df_add[df_add["type"] == "projection"]
            min_years = projection_rows.groupby(["country", "field"])["year"].min().reset_index()

            # Add a new row for each country and field with year - 1
            new_rows = min_years.copy()
            new_rows["year"] = new_rows["year"] - 1

            new_rows["type"] = "projection"

            # Add the new rows to the merged DataFrame
            df_add = pd.concat([df_add, new_rows], ignore_index=True)

        elif "complete" not in df_add.columns:
            df_add["type"] = "estimate"
        # Normalize 'field' capitalization
        df_add["field"] = df_add["field"].apply(lambda s: s[0] + s[1:].lower() if isinstance(s, str) else s)
        all_dfs_list.append(df_add)

    # Merge all dataframes on the 'year', 'country', and 'field' columns
    merged_df = all_dfs_list[0]
    for df in all_dfs_list[1:]:
        merged_df = pd.merge(merged_df, df, on=["year", "country", "field", "type"], how="outer")
    return merged_df
