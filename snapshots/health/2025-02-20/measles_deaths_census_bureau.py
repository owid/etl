"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Data for 1949
DATA_1949 = pd.DataFrame(
    {
        "country": ["United States"],
        "year": [1949],
        "deaths": [949],
        "source": ["https://www.census.gov/library/publications/1952/compendia/statab/73ed.html"],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/measles_deaths_census_bureau.csv")
    df = DATA_1949

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
