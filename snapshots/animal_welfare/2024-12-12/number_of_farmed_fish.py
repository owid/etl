"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# File names of snapshots.
FILE_NAMES = [
    "number_of_farmed_fish_2015.xlsx",
    "number_of_farmed_fish_2016.xlsx",
    "number_of_farmed_fish_2017.xlsx",
]
# Base URL for the FishCount data from 2020 onwards.
BASE_URL = "https://fishcount.org.uk/estimates/farmedfishes/data01/fishcount_global_farmed_fish_estimate.php"
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


########################################################################################################################
# NOTE: For now we will not use this, and simply extract total counts per year and country.
def get_countries():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    countries = []

    # Locate the country selection dropdown specifically.
    select_country = soup.find("select", {"name": "selcountry"})
    if select_country:
        for option in select_country.find_all("option"):
            value = option.get("value")
            if value and not value.startswith("*") and value != "pleaseselect":
                countries.append(value)

    return countries


def extract_fish_counts(year, country):
    """Extract the number of fish of each species killed in a given year in a given country."""
    params = {
        "selyear": year,
        "selcountry": country,
        "selspecies": "* All species *",
        "selsort": "Number",
    }
    response = requests.get(BASE_URL, params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    # Locate the main table containing data.
    tables = soup.find_all("table", {"style": "width: 80%"})

    # Filter the relevant table that contains species data.
    data_table = None
    for table in tables:
        if table.find("td", string=lambda t: t and "Species" in t):
            data_table = table
            break

    if not data_table:
        raise ValueError("Data table not found in the HTML content.")

    # Extract rows of data.
    data_rows = []
    for row in data_table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) > 1:
            row_data = [cell.get_text(strip=True) for cell in cells]
            data_rows.append(row_data)

    # Create DataFrame and drop duplicates.
    df = pd.DataFrame(data_rows).iloc[:, :8]
    df = df.drop_duplicates()

    return df


def extract_fish_counts_for_year(year):
    """Extract all data for a given year."""
    countries = get_countries()
    from tqdm.auto import tqdm

    data = []
    for country in tqdm(countries):
        _df = extract_fish_counts(year, country)
        _df["year"] = year
        _df["country"] = country
        data.append(_df)

    df = pd.concat(data).reset_index(drop=True)

    return df


########################################################################################################################


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Data for 2015, 2016 and 2017 comes from dedicated xlsx files.
    for file_name in FILE_NAMES:
        # Create a new snapshot.
        snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/{file_name}")

        # Download the data and save it to the snapshot.
        snap.create_snapshot(upload=upload)

    # Data from 2020 needs to be extracted from their site.
    snap = Snapshot(f"animal_welfare/{SNAPSHOT_VERSION}/number_of_farmed_fish_from_2020.csv")

    # Extract data for each year and concatenate it into one dataframe.
    df = pd.concat([extract_total_counts(year).assign(**{"year": int(year)}) for year in YEARS])

    # Save the data to the snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
