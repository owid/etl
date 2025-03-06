"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 5,
        "year": [1944, 1945, 1946, 1947, 1948],
        "deaths": [1878, 1752, 1241, 1954, 1146],
        "source": [
            "https://www.census.gov/library/publications/1946/compendia/statab/67ed.html",
            "https://www.census.gov/library/publications/1947/compendia/statab/68ed.html",
            "https://www.census.gov/library/publications/1948/compendia/statab/69ed.html",
            "https://www.census.gov/library/publications/1949/compendia/statab/70ed.html",
            "https://www.census.gov/library/publications/1950/compendia/statab/71ed.html",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"us_census_bureau/{SNAPSHOT_VERSION}/pertussis_deaths.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
