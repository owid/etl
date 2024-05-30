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
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/fao_2000.csv")

    # Data manually extracted.
    data = """
country,1934-38,1946-49
Uganda,,2100
Cambodia,,1560
Mexico,1800,2050
Peru,1860,1920
    """
    # Note that I removed "Kenya,2230," because, as the footnote says, it includes Uganda.
    # I also removed the first point of Cambodia because it was actually referring to French Indochina.
    # I also removed the first point of India because it was actually referring to India and Pakistan.
    # Note that the footnote of the table says that the year ranges for India, China and Brazil are different.
    # Create an additional dataframe for them.
    data_additional = """
country,1934-38,1946-49,1931-37,1949-50,1935-39
China,,,2230,2030
India,,,,1700
Brazil,,2340,,,2150
    """

    # Create a dataframe with the extracted data.
    df = pd.read_csv(StringIO(data))
    df_additional = pd.read_csv(StringIO(data_additional))
    df = pd.concat([df, df_additional], ignore_index=True)

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
