"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA = pd.DataFrame(
    {
        "country": ["France"] * 7,
        "year": [
            1997,
            2000,
            2003,
            2006,
            2009,
            2012,
            2020,
        ],
        "obesity_share": [8.5, 10.1, 11.9, 13.1, 14.5, 15.0, 17],
        "source": [
            "https://www.mdpi.com/2077-0383/12/3/925",
        ]
        * 7,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/obepi_resp.csv")
    df = DATA
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
