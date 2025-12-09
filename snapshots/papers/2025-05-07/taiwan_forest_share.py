"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": [*["Taiwan"] * 9],
        "year": [1904, 1926, 1956, 1982, 1994, 2000, 2005, 2010, 2015],
        "forest_share": [60, 64, 58, 57, 68, 66, 67, 69, 67],
        "source": [
            *["https://www.nature.com/articles/s41598-019-40063-1"] * 9,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/taiwan_forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
