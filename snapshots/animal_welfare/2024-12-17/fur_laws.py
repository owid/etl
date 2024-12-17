"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URL of the published Google Sheet.
DATA_URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vQdZEyJfdG9ErXc6ynqK9j6fD7yU957urqiATFHuz0xbW0kSBZJb5NUeAoSvEhahRnOcraN1smSykrE/pubhtml"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/fur_laws.csv")

    # Read data from their public Google Sheet.
    df = pd.read_html(DATA_URL, skiprows=1)[0]

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
