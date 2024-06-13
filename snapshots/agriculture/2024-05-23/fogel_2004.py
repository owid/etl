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
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/fogel_2004.csv")

    # Data manually extracted.
    data = """
Year,France,Great Britain
1700,,2095
1705,1657,
1750,,2168
1785,1848,
1800,,2237
1803-12,1846,
1845-54,2480,
1850,,2362
1909-13,,2857
1935-39,2975,
1954-55,2783,3231
1961,,3170
1965,3355,3304
1989,3465,3149

    """

    # Create a dataframe with the extracted data.
    df = pd.read_csv(StringIO(data))

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
