"""Script to create a snapshot of dataset."""

import json
import os
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


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """
    Fetch and process healthcare data from the European Society of Cardiology (ESC) website.
    To fetch data and create a snapshot, you need to first create a dictionary with links and titles of the datasets:
        HTML_DICTIONARY = {
            "Dataset Title 1": "https://example.com/dataset1",
            "Dataset Title 2": "https://example.com/dataset2",
        }
    """
    # Create a new snapshot.
    snap = Snapshot(f"cardiovascular_diseases/{SNAPSHOT_VERSION}/esc.csv")
    # Attempt to fetch data from the source URL.
    dfs = []

    for title, url_download in tqdm(HTML_DICTIONARY.items(), desc="Fetching data from the ESC website"):  # type: ignore
        response = requests.get(url_download)

        # Check if the request was successful (Status Code: 200)
        if response.status_code == 200:
            # Parse the HTML content of the page with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the table
            table = soup.find("table", {"class": "active", "id": "datatable_dataTest"})
            # Extract data
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

    df_to_file(all_dfs, file_path=snap.path)

    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
