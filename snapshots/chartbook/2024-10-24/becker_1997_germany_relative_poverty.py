"""
Script to create a snapshot of dataset.

The file comes from Becker (1997), hard to find online. (Though maybe is this [P15]: https://publikationen.ub.uni-frankfurt.de/opus4/frontdoor/deliver/index/docId/8047/file/AP_09.pdf)
I use a csv file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1g8gGUHRye1L7hEu3HqYgM56RvjHrc3LD85kBwRZcYj4/edit?gid=1888715824#gid=1888715824
After creating the file, run
    python snapshots/chartbook/2024-10-24/becker_1997_germany_relative_poverty.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/becker_1997_germany_relative_poverty.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
