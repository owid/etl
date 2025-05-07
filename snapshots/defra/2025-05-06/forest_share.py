"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": ["England"] * 3,
        "year": [
            1086,
            1350,
            1650,
        ],
        "forest_share": [15, 10, 8],
        "source": [
            "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/221023/pb13871-forestry-policy-statement.pdf"
        ]
        * 3,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"defra/{SNAPSHOT_VERSION}/forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
