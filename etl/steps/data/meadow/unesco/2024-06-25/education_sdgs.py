"""Load a snapshot and create a meadow dataset."""

import os
import zipfile

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def read_csv_from_zip(zip_path, csv_file):
    extract_dir = os.path.dirname(zip_path)

    # Open the ZIP archive
    with zipfile.ZipFile(zip_path, "r") as z:
        # Check if the CSV file exists in the ZIP archive
        if csv_file not in z.namelist():
            raise ValueError(f"{csv_file} not found in the ZIP archive")

        # Extract the file from the ZIP archive
        z.extract(csv_file, extract_dir)

    # Create the full file path
    file_path = os.path.join(extract_dir, csv_file)

    # Read the file
    df = pr.read_csv(file_path, low_memory=False)

    return df


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("education_sdgs.zip")

    # Read the relevantCSV files from the ZIP archive
    national_df = read_csv_from_zip(snap.path, "SDG_DATA_NATIONAL.csv")
    regional_df = read_csv_from_zip(snap.path, "SDG_DATA_REGIONAL.csv")
    label_df = read_csv_from_zip(snap.path, "SDG_LABEL.csv")

    #
    # Process data.
    #
    # Rename columns with regions and countries for the purpose of merging the dataframes later on
    rename_dict = {"region_id": "country", "country_id": "country"}
    regional_df.rename(columns=rename_dict, inplace=True)
    national_df.rename(columns=rename_dict, inplace=True)

    # Concatenate and merge dataframes with regional and national data
    cobimbed_df = pr.concat([regional_df, national_df], axis=0)
    label_df.columns = label_df.columns.str.lower()

    # Add indicator label columnn that provides a better description of the indicator
    df_with_labels = pr.merge(cobimbed_df, label_df, on="indicator_id", how="left")

    # Create a new table and add relevant metadata.
    tb = Table(df_with_labels, short_name=paths.short_name)
    tb = tb.format(["country", "year", "indicator_id"])
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
