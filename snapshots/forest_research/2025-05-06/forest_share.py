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
        "country": [*["England"] * 14, *["Scotland"] * 18],
        "year": [
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
            1870,
            1874,
            1881,
            1887,
            1889,
            1895,
            1903,
            1910,
            1919,
            1931,
            1940,
            1960,
            1971,
            1981,
            1988,
            1990,
            1994,
            2000,
        ],
        "forest_share": [
            4.8,
            5.1,
            5.7,
            6.1,
            6.1,
            5.6,
            5.6,
            6,
            6.3,
            6.7,
            7,
            7.3,
            8,
            8.5,
            4.5,
            4.7,
            5,
            5.3,
            5.3,
            5.3,
            5.3,
            5.2,
            5.5,
            5.9,
            6.1,
            8,
            9.7,
            11.9,
            14.3,
            14.8,
            15.9,
            17.1,
        ],
        "source": [
            *[
                "https://www.forestresearch.gov.uk/tools-and-resources/national-forest-inventory/national-inventory-of-woodland-and-trees/national-inventory-of-woodland-and-trees-england/"
            ]
            * 14,
            *[
                "https://www.forestresearch.gov.uk/tools-and-resources/national-forest-inventory/national-inventory-of-woodland-and-trees/national-inventory-of-woodland-and-trees-scotland/"
            ]
            * 18,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"forest_research/{SNAPSHOT_VERSION}/forest_share.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
