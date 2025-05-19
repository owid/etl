"""Script to create a snapshot of dataset."""

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
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    data = download_data_table()
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/tuberculosis.csv")

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=data)


def download_data_table() -> pd.DataFrame:
    # URL of the webpage with the TB table
    url = "https://www.cdc.gov/tb-surveillance-report-2023/tables/table-1.html"  # Example: update with the actual URL

    # Send a GET request
    response = requests.get(url)
    response.raise_for_status()

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Locate the table
    table = soup.find("table")

    # Extract table rows
    data = []
    for tr in table.find_all("tr")[3:]:  # Data starts from the 4th row
        tds = tr.find_all("td")
        if tds:
            row = [td.get_text(strip=True) for td in tds]
            data.append(row)
    column_names = [
        "Year",
        "TB Cases No.",
        "TB Cases Rate",
        "TB Cases % Change No.",
        "TB Cases % Change Rate",
        "Spacer",
        "TB Deaths No.",
        "TB Deaths Rate",
        "TB Deaths % Change No.",
        "TB Deaths % Change Rate",
    ]
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    # Get the first four digits of the year and convert to int (there are some years with superscripts that are being incorrectly parsed)
    df["Year"] = df["Year"].str.extract(r"(\d{4})")[0].astype(int)

    return df


if __name__ == "__main__":
    run()
