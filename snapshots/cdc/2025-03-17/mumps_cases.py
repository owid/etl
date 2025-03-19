"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 55,
        "year": list(range(1967, 2022)),
        "cases": [
            152209,
            90918,
            104953,
            124939,
            74215,
            69612,
            59128,
            59647,
            38492,
            21436,
            16817,
            14225,
            8576,
            4941,
            5270,
            3355,
            3021,
            2982,
            7790,
            12848,
            4866,
            5712,
            5292,
            4264,
            2572,
            1692,
            1537,
            906,
            751,
            683,
            666,
            387,
            338,
            266,
            270,
            231,
            258,
            314,
            6584,
            800,
            454,
            1991,
            2612,
            404,
            229,
            584,
            1223,
            1329,
            6369,
            6109,
            2515,
            3780,
            694,
            189,
            386,
        ],
        "source": [
            *["https://www.cdc.gov/mmwr/preview/index93.html"] * 26,
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
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/mumps_cases.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
