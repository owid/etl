"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": ["England", "Scotland"],
        "year": [
            2019,
            2019,
        ],
        "forest_share": [
            10,
            18,
        ],
        "source": [
            "https://www.gov.scot/binaries/content/documents/govscot/publications/strategy-plan/2019/02/scotlands-forestry-strategy-20192029/documents/scotlands-forestry-strategy-2019-2029/scotlands-forestry-strategy-2019-2029/govscot%3Adocument/scotlands-forestry-strategy-2019-2029.pdf",
        ]
        * 2,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/scottish_government.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
