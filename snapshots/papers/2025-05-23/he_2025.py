"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": ["China"] * 22,
        "year": [
            1000,
            1050,
            1100,
            1150,
            1200,
            1250,
            1300,
            1350,
            1400,
            1450,
            1500,
            1550,
            1600,
            1650,
            1700,
            1750,
            1800,
            1850,
            1900,
            1950,
            1960,
            2000,
        ],
        "forest_share": [
            31,
            30.5,
            30.2,
            29.7,
            29.1,
            28.6,
            28.2,
            27.8,
            27.4,
            26.9,
            26.6,
            26.1,
            26,
            24.9,
            23.5,
            22.4,
            20.6,
            18.1,
            15.3,
            11.5,
            9.3,
            16,
        ],
        "source": [
            "https://link.springer.com/article/10.1007/s11430-024-1454-4",
        ]
        * 22,
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/he_2025.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
