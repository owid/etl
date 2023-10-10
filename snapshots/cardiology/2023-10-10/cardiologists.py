"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cardiology/{SNAPSHOT_VERSION}/cardiologists.csv")
    # Attempt to fetch data from the source URL.
    if snap.metadata.origin is not None:
        response = requests.get(snap.metadata.origin.url_download)

        # Check if the request was successful (Status Code: 200)
        if response.status_code == 200:
            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the table
            table = soup.find("table", {"class": "active", "id": "datatable_dataTest"})

            # Extract data
            data = []
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip the header row
                columns = row.find_all("td")
                if len(columns) > 1:
                    country = columns[0].get_text(strip=True)
                    year = columns[2].get_text(strip=True)
                    number = columns[3].get_text(strip=True).split(" ")[0]  # Considering only the numeric part

                    data.append([country, year, number])
            # Create a DataFrame
            df = pd.DataFrame(data, columns=["country", "year", "number_of_cardigologists_per_million"])
            df_to_file(df, file_path=snap.path)

            # Download data from source, add file to DVC and upload to S3.
            snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
