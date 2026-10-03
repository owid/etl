"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Data for 1937 - 2022 (except 1938-43 inclusive as that's from the census bureau)
DATA_CDC = pd.DataFrame(
    {
        "country": "United States",
        "year": 1941,
        "cases": [9086],
        "source": [
            "https://www.census.gov/library/publications/1945/compendia/statab/66ed.html",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"us_census_bureau/{SNAPSHOT_VERSION}/polio_cases.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
