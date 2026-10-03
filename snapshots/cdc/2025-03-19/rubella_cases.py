"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 57,
        "year": list(range(1966, 2023)),
        "cases": [
            46975,
            46888,
            49371,
            57686,
            56552,
            45086,
            25507,
            27804,
            11917,
            16652,
            12491,
            20395,
            18269,
            11795,
            3904,
            2077,
            2325,
            970,
            752,
            630,
            551,
            306,
            225,
            396,
            1125,
            1401,
            160,
            192,
            227,
            128,
            238,
            181,
            364,
            267,
            176,
            23,
            18,
            7,
            10,
            11,
            11,
            12,
            16,
            3,
            5,
            4,
            9,
            9,
            6,
            5,
            1,
            7,
            4,
            6,
            6,
            7,
            7,
        ],
        "source": [
            *["https://www.cdc.gov/mmwr/preview/index93.html"] * 28,
            *["https://www.cdc.gov/mmwr/preview/mmwrhtml/mm5853a1.htm"] * 7,
            *["https://www.jstor.org/stable/23317332"] * 6,
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
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/rubella_cases.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
