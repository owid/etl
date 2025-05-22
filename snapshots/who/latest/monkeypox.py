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


WHO_REGIONS = ["EURO", "AMRO", "WPRO", "EMRO", "AFRO", "SEARO"]


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/monkeypox.csv")
    df = get_shiny_data()
    # df = pd.DataFrame()
    # Fetching the data for each WHO region separately
    # for region in WHO_REGIONS:
    #    url = f"https://xmart-api-public.who.int/MPX/V_MPX_VALIDATED_DAILY?&$format=csv?&$format=csv&$filter=WHO_REGION%20eq%20%27{region}%27"
    #    df_region = pd.read_csv(url)
    #    df = pd.concat([df, df_region])

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


def get_shiny_data():
    url = "https://worldhealthorg.shinyapps.io/mpx_global/#daily-data-same-format-as-deprecated-api"

    # Step 1: Download page content
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        section = soup.find(id="daily-data-same-format-as-deprecated-api")

        # Then find the button inside it
        if section:
            button = section.find("button")
            if not button:
                print("No button found in section.")
        else:
            print("Section not found.")
        # Step 2: Find the button with base64 CSV data

        if button and "onclick" in button.attrs:
            onclick = button["onclick"]

            # Step 3: Extract base64 string
            if "data:text/csv;base64," in onclick:
                base64_data = onclick.split("data:text/csv;base64,")[1]
                base64_data = base64_data.strip("')")  # Clean up end characters

                # Step 4: Decode and parse CSV
                csv_bytes = base64.b64decode(base64_data)
                csv_str = csv_bytes.decode("latin1", errors="ignore")  # Clean problematic characters
                df = pd.read_csv(StringIO(csv_str))

                return df
    else:
        print(f"Failed to retrieve the page: {response.status_code}")

    return None


if __name__ == "__main__":
    main()
