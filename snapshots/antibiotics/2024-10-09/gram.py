"""Script to create a snapshot of dataset."""

from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/gram.csv")
    # The Shiny App needs to be running to download the data - so we have to use this method.
    url = "https://livedataoxford.shinyapps.io/GRAM_antibiotic_consumption/_w_9e3758c3/session/d74881a25060fa0b755ced23a5bb66a9/download/download_output?w=9e3758c3"
    response = requests.get(url)
    assert response.status_code == 200, "Failed to download the file"
    # Convert the content to a pandas DataFrame
    data = StringIO(response.text)
    df = pd.read_csv(data)
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
