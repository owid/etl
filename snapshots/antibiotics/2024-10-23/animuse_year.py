"""Script to create a snapshot of dataset.

To get this data you must follow these steps:

- Go here: https://amu.woah.org/amu-system-portal/amu-data
- Click 'HOME'
- In the Interactive Report click 'Year analysis'
- Click 'mg/kg'
- I compiled each separately and then manually combined them
Then:
- Click 'Covered Animal Biomass'
- In the same sheet as above, - I compiled each region separately and then manually combined them
- Upload this file
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
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/animuse_year.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
