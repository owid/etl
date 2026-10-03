"""Script to create a snapshot of dataset."""

from io import StringIO
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
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/jonsson_1998.csv")

    # Data extracted using chatGPT 4o (and manually inspected and corrected).
    data = """
year,daily_calories
1770,3048
1784,2322
1795,2724
1819,2887
1840,3080
1849,3381
1855,2917
1863,2885
1870,2573
1880,3002
1890,3106
1900,3316
1910,3499
1920,3610
1930,4207
1938,4066
    """

    # Create a dataframe with the extracted data.
    df = pd.read_csv(StringIO(data))

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
