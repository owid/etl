"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Data for 1919
DATA_1919_1940 = pd.DataFrame(
    {
        "country": ["United States"] * 8,
        "year": [1919, 1921, 1924, 1925, 1937, 1938, 1939, 1940],
        "deaths": [12992, 3370, 8370, 2309, 1395, 3227, 1171, 681],
        "source": [
            "https://www.jstor.org/stable/4575902",
            "https://www.jstor.org/stable/4576538",
            "https://www.jstor.org/stable/4577735",
            "https://www.jstor.org/stable/4578131",
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
    snap = Snapshot(f"health/{SNAPSHOT_VERSION}/measles_deaths_public_health_reports.csv")
    df = DATA_1919_1940
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
