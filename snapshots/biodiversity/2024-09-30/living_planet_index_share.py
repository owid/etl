"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    df = pd.DataFrame(
        {
            "country": ["World", "World", "World", "World", "World"],
            "taxon": ["total", "mammals", "fish", "birds", "herptiles"],
            "year": [2022, 2022, 2022, 2022, 2022],
            "share_increasing": [43.3, 44.9, 44.6, 41.5, 43.3],
            "share_decreasing": [50.0, 45.6, 51.0, 51.3, 51.6],
            "share_stable": [6.7, 9.5, 4.4, 7.3, 5.2],
            "share_strong_increase": [pd.NA, 34.8, 38.3, 33.5, 36.6],
            "share_moderate_increase": [pd.NA, 7.1, 4.4, 4.5, 4.6],
            "share_little_change": [pd.NA, 15.4, 8.5, 14.3, 9.6],
            "share_moderate_decrease": [pd.NA, 6.0, 4.7, 5.7, 5.3],
            "share_strong_decrease": [pd.NA, 36.7, 44.2, 42, 44],
        }
    )
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/living_planet_index_share.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
