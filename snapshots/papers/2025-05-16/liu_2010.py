"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": ["China"] * 6,
        "year": [1700, 1800, 1900, 1980, 2000, 2005],
        "forest_share": [18.8, 16.5, 13.1, 13.3, 14.7, 14.7],
        "source": [
            "https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2009GB003687",
        ]
        * 6,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/liu_2010.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()


938.821
