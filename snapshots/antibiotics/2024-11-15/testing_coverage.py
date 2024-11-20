"""Script to create a snapshot of dataset.

To download the data visit:

1. https://worldhealthorg.shinyapps.io/glass-dashboard/_w_c75b737c/#!/amr

2. Scroll to the 'Testing coverage by infectious syndrome' section and download the data for each region.

3. Then zip this up and upload the file to snapshot.

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
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/testing_coverage.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
