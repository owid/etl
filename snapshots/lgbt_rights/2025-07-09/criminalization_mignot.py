"""
Script to create a snapshot of dataset.
The data was updated by hand using the previous version of the data (2024-06-11) and the following changes from the author:

DECRIMINALIZED

in 2020: Gabon
in 2021: Angola, Bhutan
in 2022: Antigua and Barbuda, Barbados
in 2023: Cook Islands, Mauritius, Singapore
in 2024: Dominica, Namibia


(RE)CRIMINALIZED

in 2025: Trinidad and Tobago

The data was provided by email.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"lgbt_rights/{SNAPSHOT_VERSION}/criminalization_mignot.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
