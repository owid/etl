"""Script to create a snapshot of dataset 'Short-Term Mortality Fluctuations (HMD, 2023)'.

To run the script, you have to set the following env variables with your credentials:
- SNAPSHOTS_HMD_STMF_EMAIL
- SNAPSHOTS_HMD_STMF_PASSWORD
"""

import os
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

    # Download CSV
    download_csv(snap.path, snap.m.source.source_data_url)  # type: ignore

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def download_csv(path: Path, csv_url: str) -> None:
    """Download CSV file from source and save it to the given path.
    We need to login first, get session cookie and then use it to
    download the CSV file."""

    # Create a session object to persist cookies and headers
    session = requests.Session()

    # URL for login
    login_url = "https://www.mortality.org/Account/Login"

    # Step 1: Load the login page to get the dynamic anti-forgery token.
    login_page_response = session.get(login_url)
    assert login_page_response.ok, f"Failed to load the login page: {login_page_response.status_code}"

    # Parse the login page HTML to extract the __RequestVerificationToken
    soup = BeautifulSoup(login_page_response.text, "html.parser")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    assert token_input is not None, "Could not find the anti-forgery token on the login page."

    token_value = token_input.get("value")  # type: ignore

    # Step 2: Prepare login form data with the dynamic token.
    login_data = {
        "ReturnUrl": "https://www.mortality.org/Home/Index",
        "Email": os.environ["SNAPSHOTS_HMD_STMF_EMAIL"],
        "Password": os.environ["SNAPSHOTS_HMD_STMF_PASSWORD"],
        "__RequestVerificationToken": token_value,
    }

    # Step 3: Perform the login POST request with the dynamic token.
    login_response = session.post(login_url, data=login_data)
    assert login_response.ok, "Login successful."

    # Optionally, check if an Authorization cookie was set.
    assert "Authorization" in session.cookies

    # Step 4: Fetch the CSV file using the authenticated session.
    csv_response = session.get(csv_url)
    assert csv_response.ok, f"Failed to download CSV file: {csv_response.status_code}"
    with open(path, "wb") as file:
        file.write(csv_response.content)


def modify_metadata(snap: Snapshot) -> Snapshot:
    """Modify metadata"""
    # Get access date
    snap.metadata.source.date_accessed = date.today()  # type: ignore
    # Set publication date
    publication_date = _get_publication_date()
    assert snap.metadata.source
    snap.metadata.source.publication_date = publication_date  # type: ignore
    # Set publication year
    snap.metadata.source.publication_year = publication_date.year
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
    match1 = re.search(r"Last update: (\d\d-\d\d-20\d\d)", date_raw)
    match2 = re.search(r"Last update: (\d\d/\d\d/20\d\d)", date_raw)
    if match1:
        date_str = match1.group(1)
        date_format = "%d-%m-%Y"
    elif match2:
        date_str = match2.group(1)
        date_format = "%m/%d/%Y"
    else:
        raise ValueError("No match was found! RegEx did not work, perhaps the date format has changed.")
    # Format date YYYY-MM-DD
    return datetime.strptime(date_str, date_format).date()


if __name__ == "__main__":
    main()
