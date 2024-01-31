"""Script to create a snapshot of dataset."""

import os
import tempfile
from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.

    df = create_table_for_recent_years()
    snap = Snapshot(f"biodiversity/{SNAPSHOT_VERSION}/cherry_blossom.xls")

    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "cherry_blossom.csv"
        df.to_csv(output_file)

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload, filename=output_file)

        # Remove the file in the temporary directory.
        os.remove(output_file)


def create_table_for_recent_years() -> pd.DataFrame:
    """
    Create a table for cherry blossom full flowering dates for recent years not covered in the original downloadable xls dataset.

    This function manually adds data for the years 2016 to 2023. The data for 2016 to 2021 is sourced from the official dataset [website](http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/),
    while the data for 2022 and 2023 comes from personal communication with the dataset author.
    Returns:
        Table: The table with added data for recent years.
    """

    df = pd.DataFrame(
        {
            "country": ["Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan", "Japan"],
            "year": ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"],
            "Full-flowering date": ["404", "409", "330", "405", "401", "326", "401", "325"],
        }
    )

    return df


if __name__ == "__main__":
    main()
