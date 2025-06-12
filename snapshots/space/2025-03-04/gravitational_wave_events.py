"""Script to create a snapshot of dataset.

NOTE: The publication date can be set as the date of the latest publication in:
https://zenodo.org/communities/ligo-virgo-kagra/records?q=&f=allversions%3Atrue&l=list&p=1&s=10&sort=newest

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
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/gravitational_wave_events.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
