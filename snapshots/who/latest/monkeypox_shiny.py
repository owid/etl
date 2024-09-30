"""Script to create a snapshot of dataset."""

import base64
from io import StringIO
from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/monkeypox_shiny.csv")

    df = get_shiny_data()

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


def get_shiny_data():
    # URL of the webpage
    url = "https://worldhealthorg.shinyapps.io/mpx_global/#26_Case_definitions"

    # Fetch the page content
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the button tag with the specific class (for example)
        button = soup.find("button", class_="btn btn-primary")

        # Extract the value of the 'onclick' attribute
        if button:
            onclick_value = button.get("onclick")
            # If the attribute contains 'data:text/csv;base64,', extract and decode it - should make this a bit more stable to ensure it can only download the button we want
            if "data:text/csv;base64," in onclick_value:
                base64_data = onclick_value.split("data:text/csv;base64,")[1].strip("')")
                base64_data = base64_data.split(")")[0]
                decoded_csv = base64.b64decode(base64_data).decode("utf-8")
                csv_data = StringIO(decoded_csv)
                df = pd.read_csv(csv_data)
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
    return df


if __name__ == "__main__":
    main()
