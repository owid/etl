"""Script to create a snapshot of dataset."""

import tempfile
from pathlib import Path
from time import sleep
from typing import Optional
from zipfile import ZipFile

import click
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.catalog.utils import underscore
from owid.datautils.web import download_file_from_url
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Time to wait between consecutive queries.
TIME_BETWEEN_QUERIES = 0.1


def get_table_of_commodities_and_urls(url_main: str) -> pd.DataFrame:
    response = requests.get(url_main)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table by class.
    table = soup.find("table", class_="usa-table usa-table--striped")

    # Extract the rows
    data = []
    for row in table.find_all("tr")[2:]:
        cols = row.find_all("td")
        commodity = cols[0].get_text(strip=True) if len(cols) > 0 else "NA"
        supply_demand_url = cols[1].find("a")["href"] if len(cols) > 1 and cols[1].find("a") else "NA"
        supply_demand_year_update = cols[2].get_text(strip=True) if len(cols) > 2 else "NA"
        end_use_url = cols[4].find("a")["href"] if len(cols) > 4 and cols[4].find("a") else "NA"
        end_use_year_update = cols[5].get_text(strip=True) if len(cols) > 5 else "NA"

        # Add row to data regardless of whether URLs are present
        data.append(
            [
                commodity,
                supply_demand_url,
                supply_demand_year_update,
                end_use_url,
                end_use_year_update,
            ]
        )

    # Create a DataFrame with the fetched data.
    df = pd.DataFrame(
        data,
        columns=["commodity", "supply_demand_url", "supply_demand_year_update", "end_use_url", "end_use_year_update"],
    )

    # Some urls start with "/media"; identify the idx of the rows with such urls.
    commodities_without_file_index = df[df["supply_demand_url"].str.startswith("/media")].index
    for idx in tqdm(commodities_without_file_index):
        df.loc[idx, "supply_demand_url"] = _fetch_file_url_from_media_path(df.loc[idx, "supply_demand_url"])

    # Sanity checks.
    error = "Some commodities are missing a URL for their supply-demand file."
    assert (
        (df["supply_demand_url"].str.startswith("https://d9-wret.s3.us-west-2.amazonaws.com/"))
        | (df["supply_demand_url"] == "NA")
    ).all(), error

    error = "Some commodities are missing a URL for their end-use file."
    assert (
        (df["end_use_url"].str.startswith("https://d9-wret.s3.us-west-2.amazonaws.com/")) | (df["end_use_url"] == "NA")
    ).all(), error

    return df


def _fetch_file_url_from_media_path(media_path: str) -> Optional[str]:
    # The link to the file of some of the commodities lead to a subpage.
    # In those cases, the url is a media path, e.g. "/media/files/aluminum-historical-statistics-data-series-140"
    url = f"https://www.usgs.gov{media_path}"

    # Fetch the subpage.
    response_subpage = requests.get(url)

    # Parse the HTML content.
    soup_subpage = BeautifulSoup(response_subpage.content, "html.parser")

    # Find the div by class.
    div = soup_subpage.find("div", class_="media-full-entity")

    # Find the first link within the div.
    file_link = div.find("a", href=True)  # type: ignore

    # Extract the URL.
    data_file_url = file_link["href"] if file_link else None  # type: ignore

    # Wait before sending next query.
    sleep(TIME_BETWEEN_QUERIES)

    return data_file_url  # type: ignore


def download_all_files(df: pd.DataFrame, snapshot_path: Path) -> None:
    # Ensure the output folder exists.
    snapshot_path.parent.mkdir(exist_ok=True, parents=True)

    # Create a temporary directory.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create a temporary subdirectory for supply-demand statistics, and another for end-use statistics.
        supply_demand_dir = temp_path / "supply_demand"
        supply_demand_dir.mkdir(parents=True, exist_ok=True)
        end_use_dir = temp_path / "end_use"
        end_use_dir.mkdir(parents=True, exist_ok=True)

        # Download files for all commodities.
        for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Downloading files"):
            if row["supply_demand_url"] != "NA":
                download_file_from_url(
                    url=row["supply_demand_url"],
                    local_path=supply_demand_dir / f"{underscore(row['commodity'])}.xlsx",  # type: ignore[reportArgumentType]
                )

            if row["end_use_url"] != "NA":
                download_file_from_url(
                    url=row["end_use_url"],
                    local_path=end_use_dir / f"{underscore(row['commodity'])}.xlsx",  # type: ignore[reportArgumentType]
                )

        # Create the zip file at the snapshot path.
        with ZipFile(snapshot_path, "w") as zipf:
            # Add supply_demand and end_use files to the zip.
            for folder in [supply_demand_dir, end_use_dir]:
                for file_path in folder.rglob("*"):
                    if file_path.is_file():
                        zipf.write(file_path, file_path.relative_to(temp_path))


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize new snapshot.
    snap = Snapshot(f"usgs/{SNAPSHOT_VERSION}/historical_statistics_for_mineral_and_material_commodities.zip")

    # The main page of the dataset contains a table with a row for each commodity.
    # Each row (although not all) contains a url of supply-demand statistics and another for end-use statistics.
    # Some of those urls are a direct download link.
    # But other urls lead to another subpage that contains a download link.
    # The following function will list all commodities and their urls.
    # Then, all urls leading to subpages will be fetched and the download link extracted.
    # In the end a sanity check will ensure all urls correspond to a download link.
    # NOTE: This may take a few minutes.
    df = get_table_of_commodities_and_urls(url_main=snap.metadata.origin.url_main)  # type: ignore

    # Download the supply-demand statistics file and end-use statistics file for each commodity in a temporary folder.
    # A compressed file will be created at the end in the corresponding data/snapshots/ folder.
    # NOTE: This may take a few minutes.
    download_all_files(df=df, snapshot_path=snap.path)

    # Upload zip file to S3.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    main()
