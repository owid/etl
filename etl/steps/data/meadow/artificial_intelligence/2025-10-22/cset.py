"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
import zipfile
from typing import List

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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
        final_df = pr.merge(final_df, df, on=["year", "country", "field"], how="outer")

    tb = Table(final_df, short_name=paths.short_name, underscore=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "field"])
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    # Save changes in the new meadow dataset.
    ds_meadow.save()


def read_and_clean_data(file_ids: List[str], temp_dir: str, field_name: str):
    """
    Reads data from a list of CSV files inside a temporary directory, cleans them, and merges into a single DataFrame.
    """
    all_dfs_list = []
    for file_name in file_ids:
        # Construct the full path to the file in the temporary directory
        file_path = os.path.join(temp_dir, "cat", file_name)

        # Read the CSV file
        df_add = pr.read_csv(file_path)

        # Handle estimated investment data with projections
        if file_name == "companies_yearly_estimated.csv" and "complete" in df_add.columns:
            df_add["complete"] = df_add["complete"].astype(str)

            # Get value columns
            value_cols = [col for col in df_add.columns if col not in ["country", "year", "field", "complete"]]

            # Filter actual data (complete = True)
            df_actual = df_add[df_add["complete"] == "True"].copy()
            df_actual = df_actual.drop(columns=["complete"])
            df_actual = df_actual.dropna(subset=value_cols, how="all")

            # For projected data (complete = False), we only want the previous year's value
            df_projected_rows = df_add[df_add["complete"] == "False"].copy()
            df_projected_rows = df_projected_rows.drop(columns=["complete"])

            # Find the last actual year for each country/field
            df_last_actual = df_actual.loc[df_actual.groupby(["country", "field"])["year"].idxmax()].copy()
            # Create projected data with last actual values

            # Merge last actual values (rename columns to _projected)
            df_last_values = df_last_actual[["country", "field", "year"] + value_cols].copy()
            for col in value_cols:
                df_last_values = df_last_values.rename(columns={col: f"{col}_projected"})
                df_projected_rows = df_projected_rows.rename(columns={col: f"{col}_projected"})

            df_projected_final = pr.concat([df_projected_rows, df_last_values], ignore_index=True)

            # Combine actual and projected data
            df_add = pr.merge(df_actual, df_projected_final, on=["country", "year", "field"], how="outer")
        # Filter by 'complete' column for other datasets
        elif "complete" in df_add.columns:
            df_add["complete"] = df_add["complete"].astype(str)
            df_add = df_add[df_add["complete"] == "True"]
            df_add = df_add.drop(columns=["complete"])

        # Normalize 'field' capitalization
        df_add["field"] = df_add["field"].apply(lambda s: s[0] + s[1:].lower() if isinstance(s, str) else s)

        # Aggregate duplicates by summing numeric columns
        # Group by country, year, and field, then sum all numeric columns
        # Skip groupby for estimated investment file as it's already been processed
        if file_name != "companies_yearly_estimated.csv":
            numeric_cols = df_add.select_dtypes(include=["number"]).columns.tolist()
            if numeric_cols:
                df_add = df_add.groupby(["country", "year", "field"], as_index=False)[numeric_cols].sum()

        all_dfs_list.append(df_add)

    # Merge all dataframes on the 'year', 'country', and 'field' columns
    merged_df = all_dfs_list[0]
    for df in all_dfs_list[1:]:
        merged_df = pd.merge(merged_df, df, on=["year", "country", "field"], how="outer")
    return merged_df
