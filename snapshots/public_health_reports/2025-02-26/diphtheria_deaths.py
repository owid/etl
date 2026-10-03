"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
DATA_CDC = pd.DataFrame(
    {
        "country": ["United States"] * 4,
        "year": [
            1937,
            1938,
            1939,
            1940,
        ],
        "deaths": [2615, 2560, 2022, 1457],
        "source": [
            "https://stacks.cdc.gov/view/cdc/69651/cdc_69651_DS1.pdf",
            "https://www.jstor.org/stable/4583204",
            "https://www.jstor.org/stable/4583620",
            "https://www.jstor.org/stable/4584013",
        ],
    }
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"public_health_reports/{SNAPSHOT_VERSION}/diphtheria_deaths.csv")
    df = DATA_CDC
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
