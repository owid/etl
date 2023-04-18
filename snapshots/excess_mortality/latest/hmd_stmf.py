"""Script to create a snapshot of dataset 'Short-Term Mortality Fluctuations (HMD, 2023)'."""
import re
from datetime import date, datetime
from pathlib import Path

import click
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"excess_mortality/{SNAPSHOT_VERSION}/hmd_stmf.csv")

    # Add date_accessed
    snap = modify_metadata(snap)

    # Download data from source.
    snap.download_from_source()

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def modify_metadata(snap: Snapshot) -> Snapshot:
    """Modify metadata"""
    # Get access date
    snap.metadata.date_accessed = date.today()
    # Set publication date
    publication_date = _get_publication_date()
    snap.metadata.publication_date = publication_date
    # Set publication year
    snap.metadata.publication_year = publication_date.year
    # Save
    snap.metadata.save()
    return snap


def _get_publication_date() -> date:
    # Read source page
    html = requests.get("https://www.mortality.org/Data/STMF")
    soup = BeautifulSoup(html.text, "html.parser")
    # Get element with date text
    html_class = "updated-date"
    elements = soup.find_all(class_=html_class)
    # Obtain raw date
    if len(elements) == 1:
        date_raw = elements[0].text
    elif len(elements) == 0:
        raise ValueError(f"More than one element with class '{html_class}' was found!")
    else:
        raise ValueError(f"HTML in source may have changed. No element of class '{html_class}' was found.")
    # Extract date
    match = re.search(r"Last update: (\d\d\-\d\d\-20\d\d)", date_raw)
    if match:
        date_str = match.group(1)
    else:
        raise ValueError("No match was found! RegEx did not work, perhaps the date format has changed.")
    # Format date YYYY-MM-DD
    return datetime.strptime(date_str, "%d-%m-%Y").date()


if __name__ == "__main__":
    main()
