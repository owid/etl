"""Script to create a snapshot of dataset.

This code scrapes the UN website to get a list of all member states and the year they joined.

"""

import re
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
    snap = Snapshot(f"un/{SNAPSHOT_VERSION}/un_members.csv")

    # Fetch the web page.
    page = requests.get(snap.metadata.origin.url_main, headers={"User-Agent": "Mozilla/8.0"})
    soup = BeautifulSoup(page.content, "html.parser")

    # Find the divs containing the country data.
    divs = soup.find_all(class_="country")

    # Extract the country name and admission year.
    country = [div.find("h2").text for div in divs]
    admission = [re.search(r"\d{4}$", div.find(class_="text-muted").text).group() for div in divs]  # type: ignore

    # Check the length of the lists to make sure they are the same.
    if len(country) != len(admission):
        raise ValueError("Length mismatch between country and admission lists")

    # Create a DataFrame with the data.
    df = pd.DataFrame({"country": country, "admission": admission})

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    run()
