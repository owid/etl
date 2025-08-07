"""Script to create a snapshot of dataset.

To download go here:

- https://www.who.int/data/data-collection-tools/who-mortality-database#:~:text=The%20WHO%20Mortality%20Database%20is,death%20as%20reported%20by%20countries
- Find the folder named 'Availability' use the link for that in the URL download option in the .dvc.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/icd_codes.zip")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
