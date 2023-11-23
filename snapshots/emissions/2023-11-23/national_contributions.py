"""Script to create a snapshot of dataset 'National contributions to climate change (Jones et al. (2023), 2023)'."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of data files to snapshot.
DATA_FILES = {
    "annual_emissions.csv",
    "cumulative_emissions.csv",
    "temperature_response.csv",
}


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    for data_file in DATA_FILES:
        # Create a new snapshot.
        snap = Snapshot(f"emissions/{SNAPSHOT_VERSION}/national_contributions_{data_file}")

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
