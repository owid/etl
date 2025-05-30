"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": ["Scotland", "Scotland"],
        "year": [1600, 1750],
        "forest_share": [5, 5],
        "source": [
            "https://www.tandfonline.com/doi/abs/10.1080/00369220418737194",
            "https://www.tandfonline.com/doi/abs/10.1080/00369220418737194",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/mather_2004.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
