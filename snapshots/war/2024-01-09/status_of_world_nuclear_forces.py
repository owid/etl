"""Script to create a snapshot of dataset.

The data is directly extracted from the website.

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
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/status_of_world_nuclear_forces.csv")

    # Request HTML content from website.
    response = requests.get(snap.metadata.origin.url_main)  # type: ignore

    # Parse HTML content.
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract the title of the table.
    title = soup.find("div", class_="data-embed__title").get_text(strip=True)  # type: ignore

    # Extract the year from the title.
    year = int(re.findall("\d{4}", title)[0])  # type: ignore

    # Find the relevant table by its class.
    table = soup.find("div", class_="data-embed__embed data-embed__embed--scroll")

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
            "COUNTRY",
            "DEPLOYED STRATEGIC",
            "DEPLOYED NONSTRATEGIC",
            "RESERVE/NONDEPLOYED",
            "MILITARY STOCKPILE(A)",
            "TOTAL INV",
        ],
    )

    # Remove empty rows.
    df = df.dropna().reset_index(drop=True)

    # Add year to table.
    df["year"] = year

    # Copy data to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
