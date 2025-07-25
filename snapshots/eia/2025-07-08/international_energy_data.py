"""Script to create a snapshot of dataset 'International Energy Data'.

The date_published is written at the beginning of the text file that is generated when uncompressing the .zip file, by a field called "last_updated". It may be fair to assume that the dataset is regularly updated, so we could just use date_accessed for it.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def run(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eia/{SNAPSHOT_VERSION}/international_energy_data.zip")

    # Download data from source and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
