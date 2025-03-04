"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 7,
        "year": [1943, 1944, 1945, 1946, 1947, 1948, 1949],
        "cases": [1196, 1145, 1598, 1259, 799, 634, 574],
        "source": [
            "https://www.census.gov/library/publications/1945/compendia/statab/66ed.html",
            "https://www.census.gov/library/publications/1946/compendia/statab/67ed.html",
            "https://www.census.gov/library/publications/1947/compendia/statab/68ed.html",
            "https://www.census.gov/library/publications/1948/compendia/statab/69ed.html",
            "https://www.census.gov/library/publications/1949/compendia/statab/70ed.html",
            "https://www.census.gov/library/publications/1950/compendia/statab/71ed.html",
            "https://www.census.gov/library/publications/1952/compendia/statab/73ed.html",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"us_census_bureau/{SNAPSHOT_VERSION}/diphtheria_deaths.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
