"""Script to create a snapshot of dataset.

Data are available in GDrive links, linked to from this page: https://www.vaccineconfidence.org/vci/data-and-methodology/
"""

import os
import tempfile
from pathlib import Path

import click
import gdown
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

FILE_LINKS = [
    "https://drive.google.com/file/d/1BCvAa8MiSf9CE0J6bjwY8nHpQIrh_7A-/view?usp=drive_link",  # 2024
    "https://drive.google.com/file/d/1ZYdqDbTcf7j4S98orVLJdV19Ob28Ld_p/view?usp=drive_link",  # 2023
    "https://drive.google.com/file/d/1PJPbp8CWZkFubh648IHI18IwhmIbpmAf/view?usp=drive_link",  # 2022
    "https://drive.google.com/file/d/1Qhd2FihJ6lWRqUX5ycZl_oc-Xi7LtFQP/view?usp=drive_link",  # 2021
    "https://drive.google.com/file/d/1HvBWfXLl-QXBryeIwdRkLw2rkq1bX4Fq/view?usp=drive_link",  # 2020
    "https://drive.google.com/file/d/1_NONSpyWxGjUHdwzFUcdcQZyZLm0zoJ7/view?usp=drive_link",  # 2019
    "https://drive.google.com/file/d/1MGUpbDPXc2DM1jna4BXgc1oitFszzvEc/view?usp=drive_link",  # 2018
    "https://drive.google.com/file/d/1dVIzoFE9lHNSiFWevQPOs07YjwF9K9Lo/view?usp=drive_link",  # 2015
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/vaccine_confidence.csv")
    df = download_data(FILE_LINKS)
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


def download_data(file_links: list[str]) -> pd.DataFrame:
    dfs = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for file_link in file_links:
            # Extract the file ID from the URL.
            file_id = file_link.split("/d/")[1].split("/")[0]
            # Construct the direct download URL.
            url = f"https://drive.google.com/uc?id={file_id}"
            # Define the output file path using the file ID to create a unique name.
            output = os.path.join(temp_dir, f"downloaded_data_{file_id}.csv")
            # Download the file.
            gdown.download(url, output, quiet=False)
            # Read the downloaded CSV into a DataFrame.
            df = pd.read_csv(output)
            dfs.append(df)
        # Combine all DataFrames into one.
        combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


if __name__ == "__main__":
    main()
