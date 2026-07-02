"""Script to create a snapshot of dataset 'UK historical energy data'.

The dataset is compiled by Roger Fouquet for the National Infrastructure Commission (NIC). As it is no longer actively
maintained by the NIC, the file is retrieved from the Internet Archive's Wayback Machine (see `url_download` in the
accompanying .dvc file).

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
    snap = Snapshot(f"energy/{SNAPSHOT_VERSION}/uk_historical_energy.xlsx")

    # Download data from source to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
