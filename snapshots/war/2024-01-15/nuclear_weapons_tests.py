"""Script to create a snapshot of dataset.

The data is directly extracted from the website.

"""

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
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/nuclear_weapons_tests.csv")

    # Request HTML content from website.
    response = requests.get(snap.metadata.origin.url_main)  # type: ignore

    # Parse HTML content.
    soup = BeautifulSoup(response.text, "html.parser")

    # Extract the title of the table.
    tables = soup.find_all("table")

    # The table we are interested in is the second one, containing the number of tests for each country-year.
    table = tables[1]

    # Initialize a list to hold all rows of data.
    table_data = []
    # Iterate over all rows in the table and gather data.
    for row in table.find_all("tr"):  # type: ignore
        row_data = [cell.get_text(strip=True) for cell in row.find_all("td")]
        # Add the row data to the table data list
        table_data.append(row_data)

    # The zeroth element fetched is the left column text, which we don't need.
    # The first element is the name of the columns.
    # The last element are the notes, which we will rephrase in the metadata.
    columns = table_data[1]
    table_data = table_data[2:-1]

    # Create a dataframe with the extracted data.
    df = pd.DataFrame(table_data, columns=columns)

    # Remove empty rows.
    df = df.dropna().reset_index(drop=True)

    # Copy data to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
