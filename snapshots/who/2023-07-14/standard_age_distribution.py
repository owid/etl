"""Script to create a snapshot of dataset 'Age Standardization of Rates: A New WHO Standard '."""

from pathlib import Path

import click
import pandas as pd
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/standard_age_distribution.csv")

    df = pd.DataFrame(
        data={
            "age_min": [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100],
            "age_max": [4, 9, 14, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 69, 74, 79, 84, 89, 94, 99, None],
            "age_weight": [
                0.0886,
                0.0869,
                0.086,
                0.0847,
                0.0822,
                0.0793,
                0.0761,
                0.0715,
                0.0659,
                0.0604,
                0.0537,
                0.0455,
                0.0372,
                0.0296,
                0.0221,
                0.0152,
                0.0091,
                0.0044,
                0.0015,
                0.0004,
                0.00005,
            ],
        }
    )
    df_to_file(df, file_path=snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
