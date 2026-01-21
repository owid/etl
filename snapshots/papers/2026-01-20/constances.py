"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA = pd.DataFrame(
    {
        "country": ["France"] * 8,
        "year": [2013, 2013, 2014, 2014, 2015, 2015, 2016, 2016],
        "sex": ["female", "male", "female", "male", "female", "male", "female", "male"],
        "obesity_share": [14.27, 13.98, 15.81, 13.91, 13.66, 15.48, 15.24, 15.3],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/constances.csv")
    df = DATA
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
