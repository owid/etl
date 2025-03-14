"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 73,
        "year": list(range(1950, 2023)),
        "deaths": [
            1904,
            1551,
            3145,
            1450,
            1368,
            1043,
            566,
            221,
            255,
            454,
            230,
            90,
            60,
            41,
            17,
            16,
            9,
            16,
            24,
            13,
            7,
            18,
            2,
            10,
            3,
            9,
            16,
            16,
            13,
            4,
            6,
            0,
            0,
            0,
            0,
            3,
            0,
            0,
            1,
            0,
            0,
            1,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
        "source": [
            *["https://www.globalhealthchronicles.org/items/show/6303"] * 11,
            *["https://www.cdc.gov/mmwr/preview/index93.html"] * 33,
            *["https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm"] * 8,
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5153a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5254a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5353a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5453a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5553a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5653a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5754a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5953a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6053a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6153a1.htm",
            "https://www.cdc.gov/mmwr/preview/mmwrhtml/mm6253a1.htm",
            "https://www.cdc.gov/mmwr/volumes/63/wr/mm6354a1.htm",
            "https://www.cdc.gov/mmwr/volumes/64/wr/mm6453a1.htm",
            *["https://wonder.cdc.gov/nndss-annual-summary.html"] * 7,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/polio_deaths.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
