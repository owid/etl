"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": [*["Japan"] * 10],
        "year": [1600, 1850, 1900, 1939, 1945, 1950, 1963, 1970, 1980, 1985],
        "forest_share": [71.4, 67.5, 64.3, 62.5, 51.5, 65.9, 67.5, 66.5, 66.8, 65.6],
        "source": [
            *[
                "https://www.cambridge.org/core/journals/journal-of-global-history/article/abs/forest-history-and-the-great-divergence-china-japan-and-the-west-compared/6140D78077980694B07B40B6396C0343"
            ]
            * 10,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/japan_forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
