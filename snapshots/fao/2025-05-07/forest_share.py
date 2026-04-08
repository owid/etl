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
        "country": [*["Vietnam"] * 12],
        "year": [1943, 1976, 1980, 1985, 1990, 1995, 1999, 2002, 2003, 2004, 2005, 2006],
        "forest_share": [43, 33, 32.1, 30, 27, 28, 33.2, 35, 36.1, 36.7, 37, 38],
        "source": [
            *["https://web.archive.org/web/20230715025310/http://www.fao.org/3/am254e/am254e00.pdf"] * 12,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"fao/{SNAPSHOT_VERSION}/forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
