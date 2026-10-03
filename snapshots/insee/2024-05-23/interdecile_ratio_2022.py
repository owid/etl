"""
Script to create a snapshot of dataset.

The file was obtained from a previous version of the dataset, accessed in 2022 from the INSEE website by the Chartbook team.
You can find that file here: https://drive.google.com/file/d/1-Wk2hJo2gyelmJ32k1eZCBE-TfQcQEhD/view
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
    snap = Snapshot(f"insee/{SNAPSHOT_VERSION}/interdecile_ratio_2022.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
