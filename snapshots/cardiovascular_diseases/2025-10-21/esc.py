"""Script to create a snapshot of dataset."""

import json
import os
import time
from pathlib import Path

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils.io import df_to_file
from tqdm import tqdm  # Import tqdm for the progress bar

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Path to the json file with names and associated links to the ESC datasets
json_file_path = os.path.join(os.getcwd(), f"snapshots/cardiovascular_diseases/{SNAPSHOT_VERSION}/esc_links.json")

# Open the JSON file to get the dictionary with names and links of the ESC datasets
with open(json_file_path, "r") as json_file:
    # Use json.load() to load the contents of the file into a Python dictionary
    HTML_DICTIONARY = json.load(json_file)

# Years to fetch data for
YEARS = ["2023", "2022", "2021", "2020", "2019", "2018", "2017", "2016", "2015", "2014", "2013", "2012", "2011", "2010", "2006"]


def extract_ajax_url(soup):
    """Extract the AJAX URL template from the page HTML."""
    # Find the datatable div with data-component-url attribute
    datatable = soup.find("div", {"data-component-url": True})
    if datatable:
        component_url = datatable.get("data-component-url")
        return component_url
    return None


def fetch_data_for_year(ajax_url, year):
    """Fetch data for a specific year using the AJAX endpoint."""
    # Ensure the URL is absolute
    if ajax_url.startswith("/"):
        ajax_url = f"https://eatlas.escardio.org{ajax_url}"

    # Add year parameter to the AJAX URL
    url_with_year = f"{ajax_url}&fy={year}"

    response = requests.get(url_with_year)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"class": "active", "id": "datatable_dataTest"})

        data = []
        if table is not None:
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip the header row
                columns = row.find_all("td")
                if len(columns) > 1:
                    country = columns[0].get_text(strip=True)
                    year_text = columns[2].get_text(strip=True)
                    number = columns[3].get_text(strip=True).replace(" ", "")
                    data.append([country, year_text, number])
        return data
    return []


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """
    Fetch and process healthcare data from the European Society of Cardiology (ESC) website.
    Fetches data for all available years (2006-2023) using AJAX endpoints.
    """
    # Create a new snapshot.
    snap = Snapshot(f"cardiovascular_diseases/{SNAPSHOT_VERSION}/esc.csv")
    # Attempt to fetch data from the source URL.
    dfs = []

    for title, url_download in tqdm(HTML_DICTIONARY.items(), desc="Fetching data from the ESC website"):  # type: ignore
        # First, get the page to extract the AJAX URL
        response = requests.get(url_download)

        # Check if the request was successful (Status Code: 200)
        if response.status_code == 200:
            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract the AJAX URL template
            ajax_url = extract_ajax_url(soup)

            if ajax_url:
                # Fetch data for all years
                all_data = []
                for year in YEARS:
                    year_data = fetch_data_for_year(ajax_url, year)
                    all_data.extend(year_data)
                    # Small delay to avoid overwhelming the server
                    time.sleep(0.1)

                if all_data:
                    # Create a DataFrame
                    df = pd.DataFrame(all_data, columns=["country", "year", "value"])
                    df["indicator"] = title
                    dfs.append(df)
            else:
                # Fallback to original method if AJAX URL not found
                table = soup.find("table", {"class": "active", "id": "datatable_dataTest"})
                data = []
                if table is not None:
                    rows = table.find_all("tr")  # type: ignore
                    for row in rows[1:]:  # Skip the header row
                        columns = row.find_all("td")
                        if len(columns) > 1:
                            country = columns[0].get_text(strip=True)
                            year = columns[2].get_text(strip=True)
                            number = columns[3].get_text(strip=True).replace(" ", "")
                            data.append([country, year, number])
                    # Create a DataFrame
                    df = pd.DataFrame(data, columns=["country", "year", "value"])
                    df["indicator"] = title
                    dfs.append(df)

    all_dfs = pd.concat(dfs, ignore_index=True)

    df_to_file(all_dfs, file_path=snap.path)  # type: ignore[reportArgumentType]

    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
