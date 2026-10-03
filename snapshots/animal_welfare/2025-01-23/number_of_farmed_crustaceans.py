"""Script to extract the estimated number of farmed crustaceans killed for food from the Fishcount page."""

from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base URL for the FishCount data from 2020 onwards.
BASE_URL = "https://fishcount.org.uk/estimates/farmedcrustaceans/data02/fishcount_global_farmed_crustacean_estimate.php"

# Available years for the recent FishCount data.
YEARS = ["2020", "2021", "2022"]


def extract_total_counts(year):
    """Extract the total number of fish killed in a given year for each country."""
    # Fetch the HTML content for the given year.
    response = requests.get(
        BASE_URL + f"?selyear={year}&selcountry=pleaseselect&selspecies=*+All+species+*&selsort=Number"
    )
    # Parse the HTML content.
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all tables in the HTML.
    tables = soup.find_all("table")

    # Loop through tables to find the desired one.
    target_table = None
    for table in tables:
        # Check if the table has the specific header starting with 'Country'.
        if table.find("td", string="Country"):
            target_table = table
            break

    # Extract rows from the table.
    rows = target_table.find_all("tr")

    # Extract headers from the first row.
    headers = []
    for header in rows[0].find_all("td"):
        # Stop adding headers when encountering links or non-header data.
        if header.find("a"):
            break
        headers.append(header.get_text(strip=True).replace("<br>", " ").replace("\n", " "))

    # Extract data from subsequent rows.
    data = []
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) == len(headers):
            row_data = [
                cell.get_text(strip=True).replace(",", "").replace("\xa0", "").replace("<br>", " ") for cell in cells
            ]
            data.append(row_data)

    # Create a DataFrame with the extracted data, and remove duplicates.
    df = pd.DataFrame(data, columns=headers).drop_duplicates().reset_index(drop=True)

    return df


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Data from 2020 onwards needs to be extracted from their site.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/number_of_farmed_crustaceans.csv")

    # Extract data for each year and concatenate it into one dataframe.
    df = pd.concat([extract_total_counts(year).assign(**{"year": int(year)}) for year in YEARS])

    # Save the data to the snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
