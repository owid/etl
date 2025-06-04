"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
df = pd.DataFrame(
    {
        "country": [*["France"] * 26],
        "year": [
            1000,
            1067,
            1125,
            1185,
            1229,
            1251,
            1286,
            1329,
            1383,
            1412,
            1436,
            1480,
            1531,
            1614,
            1660,
            1688,
            1741,
            1814,
            1841,
            1871,
            1892,
            1911,
            1938,
            1955,
            1956,
            1976,
        ],
        "forest_share": [
            47,
            46,
            43,
            38,
            32,
            29,
            25,
            23,
            30,
            36,
            40,
            38,
            34,
            28,
            25,
            21,
            17,
            14,
            13,
            14,
            15,
            16,
            19,
            22,
            24,
            26,
        ],
        "source": [
            *["https://www.sciencedirect.com/science/article/abs/pii/S0743016798000230"] * 26,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"papers/{SNAPSHOT_VERSION}/france_forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
