"""Script to create a snapshot of dataset.

To create a snapshot of the dataset, follow these steps:
* Go to https://www.ipcc-nggip.iges.or.jp/EFDB/find_ef.php
* Search for "Export to XLS" in the page, and click on that button.
* Run this snapshot using the flag "--path-to-file" followed by the path to the downloaded file.

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
    snap = Snapshot(f"emissions/{SNAPSHOT_VERSION}/emission_factors.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
