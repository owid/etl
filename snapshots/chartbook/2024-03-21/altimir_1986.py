"""
Script to create a snapshot of dataset.

The snapshot file is generated manually from the paper Income distribution estimates in Argentina, 1953-1980, Table 7, available in this link
https://www.jstor.org/stable/3466844

The data is taken only from the first four columns of the table (Distribución del ingreso de los hogares, CONADE-CEPAL and Gas del Estado)
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/altimir_1986.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
