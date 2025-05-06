"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Data for 1949
df = pd.DataFrame(
    {
        "country": ["England"] * 18,
        "year": [
            1086,
            1350,
            1650,
            1870,
            1878,
            1891,
            1900,
            1910,
            1920,
            1930,
            1940,
            1949,
            1962,
            1969,
            1980,
            1990,
            2000,
            2019,
        ],
        "forest_share": [15, 10, 8, 4.8, 5.1, 5.7, 6.1, 6.1, 5.6, 5.6, 6, 6.3, 6.7, 7, 7.3, 8, 8.5, 10],
        "source": [
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/221023/pb13871-forestry-policy-statement.pdf"
        ]
        * 18,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"forests/{SNAPSHOT_VERSION}/forest_share_england.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
