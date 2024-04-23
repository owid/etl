"""Script to create a snapshot of dataset.

To access the VDEM file:

- Go to VDEM datasets: https://www.v-dem.net/data/the-v-dem-dataset/
- Go to "Country-Year: V-Dem Full+Others" page: https://www.v-dem.net/data/the-v-dem-dataset/country-year-v-dem-fullothers-v14/
- Fill the form and download the ZIP file.

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
    snap = Snapshot(f"democracy/{SNAPSHOT_VERSION}/vdem.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
