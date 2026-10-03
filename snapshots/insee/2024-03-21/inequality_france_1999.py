"""
Script to create a snapshot of dataset.
The file was created by manually creating a csv file from the data contained in the last row of the Table 10 (p. 32), in a long format.
The book is available at this link https://www.bnsp.insee.fr/ark:/12148/bc6p06xz84t/f1.pdf
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
    snap = Snapshot(f"insee/{SNAPSHOT_VERSION}/inequality_france_1999.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
