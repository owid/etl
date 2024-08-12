"""Script to create a snapshot of dataset.
Steps to get the file:
1. Go to https://www.socialstyrelsen.se/statistik-och-data/statistik/alla-statistikamnen/lagesbild-covid-19-influensa-och-rs-statistik/tidigare-publicerad-statistik/
2. Go to first chart under 'Nyinskrivningar och avlidna under pandemin' and download its data (click on 'Ladda ner data')
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
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/sweden_covid.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
