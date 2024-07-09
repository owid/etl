"""
The file has been provided by the IGH team by email on 2024-07-04. It is not available online.
Our main contacts are
    - Julia Wagner, jwagner@ighomelessness.org
    - Yamitza Yuivar, yyuivar@ighomelessness.org
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
    snap = Snapshot(f"igh/{SNAPSHOT_VERSION}/better_data_homelessness.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
