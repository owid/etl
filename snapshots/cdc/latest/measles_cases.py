"""Script to create a snapshot of dataset."""

import datetime as dt
from datetime import datetime
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cdc/{SNAPSHOT_VERSION}/measles_cases.json")
    date = get_date_of_update()
    # Update the metadata.
    snap.metadata.origin.date_published = date  # type: ignore
    snap.metadata.origin.date_accessed = dt.date.today()  # type: ignore

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


def get_date_of_update() -> str:
    """
    Get the date of the latest update for yearly measles from the CDC website - https://www.cdc.gov/measles/data-research/
    """
    url = "https://www.cdc.gov/measles/data-research/"
    response = requests.get(url)
    response.raise_for_status()  # Check that the request was successful
    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")
    # Find the div containing the date of the latest update of this data
    div = soup.find("div", {"data-section": "cdc_data_surveillance_section_5"})
    date_element = div.find("p")
    if date_element:
        date_text = date_element.get_text(strip=True)
        print(date_text)
        date_str = date_text.lower().replace("as of ", "").title()  # "February 6, 2025"
    else:
        print("Could not find the <p> tag in the target div.")

    # Parse the date string using datetime.strptime
    date_obj = datetime.strptime(date_str, "%B %d, %Y")

    # Convert to ISO format
    standard_date = date_obj.strftime("%Y-%m-%d")
    return standard_date


if __name__ == "__main__":
    main()
