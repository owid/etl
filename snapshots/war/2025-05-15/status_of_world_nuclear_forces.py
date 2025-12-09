"""Script to create a snapshot of dataset.

The data and date_published are directly extracted from the website.

"""

import re
from datetime import datetime
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
    # Create a new snapshot.
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/status_of_world_nuclear_forces.csv")

    # Request HTML content from website.
    response = requests.get(snap.metadata.origin.url_main)

    # Parse HTML content.
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the relevant table (titled "Status of World Nuclear Forces").
    table = soup.find("table", id="tablepress-2")

    # Extract the title of the table.
    section = table.find_parent("section", class_="block data-embed")
    title = section.find("div", class_="data-embed__title").get_text(strip=True)

    # Extract the year from the title.
    year = int(re.findall("\d{4}", title)[0])

    # Initialize a list to hold all rows of data.
    table_data = []
    # Iterate over all rows in the table and gather data.
    for row in table.find_all("tr"):  # type: ignore
        row_data = [cell.get_text(strip=True) for cell in row.find_all("td")]
        # Add the row data to the table data list
        table_data.append(row_data)

    # Create a dataframe with the extracted data.
    df = pd.DataFrame(
        table_data,
        columns=[
            "Country",
            "Deployed Strategic",
            "Deployed Nonstrategic",
            "Reserve/Nondeployed",
            "Military Stockpile(a)",
            "Total Inventory(b)",
        ],
    )

    # Remove empty rows.
    df = df.dropna().reset_index(drop=True)

    # Add year to table.
    df["year"] = year

    # Get the publication date.
    date_published_raw = next(
        (
            span.get_text(strip=True)
            for span in soup.find("div", class_="single-post__title-area-data").find_all("span")
            if re.match(r"\d{2}\.\d{2}\.\d{2}", span.get_text())
        ),
        None,
    )
    date_published = datetime.strptime(date_published_raw, "%m.%d.%y").strftime("%Y-%m-%d")

    # Update publication date in the snapshot metadata.
    snap.metadata.origin.date_published = date_published
    snap.metadata.save()

    # Copy data to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    run()
