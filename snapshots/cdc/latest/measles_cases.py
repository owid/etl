"""Script to create a snapshot of dataset."""

import datetime as dt
import re
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
    # Download data from source, add file to DVC and upload to S3.
    # snap.create_snapshot(upload=upload)
    snap = modify_metadata(snap, date)
    # Add the file to DVC and optionally upload it to S3, based on the `upload` parameter.
    # cdc.gov's Akamai WAF blocks browser-like User-Agents (our download default) on this
    # endpoint but allows honest client UAs, so send the real `python-requests` UA here.
    snap.create_snapshot(upload=upload, user_agent=requests.utils.default_user_agent())


def modify_metadata(snap: Snapshot, date: str) -> Snapshot:
    snap.metadata.origin.date_published = date  # ty: ignore
    snap.metadata.origin.date_accessed = dt.date.today()  # ty: ignore
    snap.metadata.save()
    return snap


def get_date_of_update() -> str:
    """
    Get the date of the latest update for yearly measles from the CDC website - https://www.cdc.gov/measles/data-research/
    """
    url = "https://www.cdc.gov/measles/data-research/"
    response = requests.get(url)
    response.raise_for_status()  # Check that the request was successful
    # Parse the HTML content
    soup = BeautifulSoup(response.content, "html.parser")
    # Each year's paragraph opens with "As of <Month> <day>, <year>, ..."; the current year's is the latest.
    # Search the whole page text, since CDC renumbers the page sections whenever it rearranges them.
    date_strs = re.findall(r"As of ([A-Z][a-z]+ \d{1,2}, \d{4})", soup.get_text(" "))
    if not date_strs:
        raise ValueError(f"Could not find an 'As of <date>' string on {url}")
    date_obj = max(datetime.strptime(date_str, "%B %d, %Y") for date_str in date_strs)

    # Convert to ISO format
    standard_date = date_obj.strftime("%Y-%m-%d")
    return standard_date


if __name__ == "__main__":
    main()
