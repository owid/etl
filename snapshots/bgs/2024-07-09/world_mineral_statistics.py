"""Script to create a snapshot with all data from BGS' World Mineral Statistics.

It may take about 20 minutes to fetch all data.
The result will be a zip folder containing a big json file, adapted to the limitations of the BGS' system.

"""

import json
import time
import zipfile
from pathlib import Path
from typing import Dict, List

import click
import requests
from bs4 import BeautifulSoup
from structlog import get_logger
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Initialize log.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base url for data queries.
URL_BASE = "https://www2.bgs.ac.uk/mineralsuk/statistics/wms.cfc"

# Seconds to wait between consecutive queries.
TIME_BETWEEN_QUERIES = 0.1


def fetch_raw_data(data_types: List[str], years: List[int], commodity_to_id: Dict[str, int]):
    # To load data for all countries we can set "country=" in the query.
    # Queries need to be in groups of maximum 10 years.
    # Example query:
    # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Imports&commodity=29&dateFrom=1970&dateTo=1979&country=&agreeToTsAndCs=agreed

    # Split years in groups of 9 years.
    years_start = [years[0]]
    for year in years[1:]:
        if year > years_start[-1] + 9:
            years_start.append(year)

    # Calculate the total number of queries required.
    # n_queries = len(years_start) * len(commodity_to_id) * len(data_types)

    # Fetch raw data in as many queries as required.
    data = {}
    for data_type in tqdm(data_types, desc="Data type"):
        data[data_type] = {}
        for commodity in tqdm(commodity_to_id, desc="Commodity"):
            data[data_type][commodity] = {}
            commodity_id = commodity_to_id[commodity]
            for year_start in years_start:
                year_end = year_start + 9

                # Construct the url for the query.
                query_url = (
                    URL_BASE
                    + f"?method=listResults&dataType={data_type}&commodity={commodity_id}&dateFrom={year_start}&dateTo={year_end}&country=&agreeToTsAndCs=agreed"
                )

                # Get response.
                response = requests.get(query_url)

                # Store content.
                data[data_type][commodity][year_start] = response.content.decode("utf-8")

                # Wait before sending next query.
                time.sleep(TIME_BETWEEN_QUERIES)

    return data


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"bgs/{SNAPSHOT_VERSION}/world_mineral_statistics.zip")

    # Load the download data page, to be able to extract the mappings of the different elements.
    page_map = requests.get(URL_BASE + "?method=searchWMS")
    soup = BeautifulSoup(page_map.text, "html.parser")

    # Map mineral names (commodities) to ids.
    soup_commodity = soup.find("select", id="commodity").find_all("option")  # type: ignore
    commodity_to_id = {option.text.strip(): int(option["value"]) for option in soup_commodity if option["value"]}

    # Data type options are simply "Imports", "Exports" and "Production".
    data_types = ["Imports", "Exports", "Production"]

    # Get the list of available years.
    soup_years = soup.find("select", id="dateFrom").find_all("option")  # type: ignore
    years = [int(option.text.strip()) for option in soup_years if option["value"]]

    # First fetch all necessary data, without processing it.
    data = fetch_raw_data(data_types=data_types, years=years, commodity_to_id=commodity_to_id)

    # Convert the dictionary to a JSON.
    json_data = json.dumps(data)

    # Ensure output snapshot folder exists, otherwise create it.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Create a zip file and add the JSON data to it
    with zipfile.ZipFile(snap.path, "w", zipfile.ZIP_DEFLATED) as zipf:
        # Add the JSON data to the zip file.
        zipf.writestr(f"{snap.metadata.short_name}.json", json_data)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    main()
