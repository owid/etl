"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 62,
        "year": list(range(1959, 2021)),
        "cases": [
            51,
            42,
            53,
            43,
            48,
            50,
            31,
            43,
            37,
            25,
            22,
            16,
            15,
            16,
            12,
            6,
            8,
            8,
            5,
            3,
            2,
            2,
            1,
            2,
            2,
            1,
            0,
            0,
            2,
            2,
            3,
            1,
            1,
            0,
            0,
            0,
            0,
            1,
            0,
            1,
            1,
            2,
            0,
            1,
            0,
            0,
            0,
            1,
            0,
            2,
            2,
            1,
            0,
            0,
            1,
            0,
            1,
            0,
            0,
            0,
            1,
            0,
        ],
        "source": [
            *["https://www.jstor.org/stable/44323897"] * 9,
            *["https://wonder.cdc.gov/cmf-icd8.html"] * 11,
            *["https://wonder.cdc.gov/cmf-icd9.html"] * 20,
            *["https://wonder.cdc.gov/cmf-icd10.html"] * 22,
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/mumps_deaths.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
