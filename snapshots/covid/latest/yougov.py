"""Script to create a snapshot of dataset."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# List of countries
## These are files per country listed in https://github.com/YouGov-Data/covid-19-tracker/tree/master/data
COUNTRIES = [
    "australia.zip",
    "brazil.csv",
    "canada.zip",
    "china.csv",
    "denmark.zip",
    "finland.csv",
    "france.zip",
    "germany.zip",
    "hong-kong.csv",
    "india.csv",
    "indonesia.csv",
    "israel.zip",
    "italy.zip",
    "japan.zip",
    "malaysia.csv",
    "mexico.csv",
    "netherlands.zip",
    "norway.zip",
    "philippines.csv",
    "saudi-arabia.csv",
    "singapore.zip",
    "south-korea.csv",
    "spain.zip",
    "sweden.zip",
    "taiwan.csv",
    "thailand.csv",
    "united-arab-emirates.csv",
    "united-kingdom.zip",
    "united-states.zip",
    "vietnam.csv",
]
EXTRA = [
    "extra_mapping.csv",
    "composite.csv",
]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    for c in COUNTRIES + EXTRA:
        # Create a new snapshot.
        snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/yougov_{c}")
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
