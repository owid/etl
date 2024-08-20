"""
Script to create a snapshot of dataset.

The file comes this pdf (page 83): https://drive.google.com/file/d/1sTz9-eXCLYFhH7nVQ0hWSvSQXFEHIxsX/view.
I use a csv file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1g8gGUHRye1L7hEu3HqYgM56RvjHrc3LD85kBwRZcYj4/edit?gid=1888715824#gid=1888715824
After creating the file, run
    python snapshots/chartbook/2024-08-20/soepmonitor_1984_2013_germany_gini.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/soepmonitor_1984_2013_germany_gini.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
