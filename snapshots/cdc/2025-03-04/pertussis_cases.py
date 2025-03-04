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
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/pertussis_cases.csv")
    df = get_data()
    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, data=df)


def get_data() -> pd.DataFrame:
    url = "https://www.cdc.gov/pertussis/php/surveillance/pertussis-cases-by-year.html"
    # Scrape data from the CDC website.
    # Fetch the webpage content
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Locate the table
    table = soup.find(
        "table", {"class": "table table-bordered show-more-div-234 table-striped main|gray-l4 nein-scroll"}
    )

    # Extract table headers
    headers = [th.text.strip() for th in table.find("thead").find_all("th")]

    # Extract table rows
    data = []
    for row in table.find("tbody").find_all("tr"):
        columns = row.find_all("td")
        if columns:
            year = row.find("th").text.strip()  # Year is in <th>
            cases = columns[0].text.strip()  # Pertussis cases
            data.append([year, cases])

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)
    df.columns = ["year", "cases"]
    df["country"] = "United States"

    return df


if __name__ == "__main__":
    main()
