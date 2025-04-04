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
            *["https://www.globalhealthchronicles.org/items/show/6303"] * 10,
            *["https://www.jstor.org/stable/44323897"] * 8,
            *["https://wonder.cdc.gov/cmf-icd8.html"] * 11,
            *["https://wonder.cdc.gov/cmf-icd9.html"] * 20,
            *["https://wonder.cdc.gov/cmf-icd10.html"] * 24,
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
