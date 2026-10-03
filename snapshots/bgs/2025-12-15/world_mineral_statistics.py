"""Script to create a snapshot with all data from BGS' World Mineral Statistics.

The BGS now uses the OGC API-Features standard for serving data.
This script fetches all records from the new API endpoint.

It may take about 20 minutes to fetch all data.
The result will be a zip folder containing a big json file.

"""

import json
import time
import zipfile
from pathlib import Path

import click
import requests
from structlog import get_logger
from tqdm.auto import tqdm

from etl.snapshot import Snapshot

# Initialize log.
log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Base url for the OGC API-Features endpoint.
API_BASE_URL = "https://ogcapi.bgs.ac.uk/collections/world-mineral-statistics/items"

# Maximum number of items to request per page (API limit).
# Testing shows the API can handle larger requests
PAGE_LIMIT = 20000

# Seconds to wait between consecutive queries.
# Reduced to speed up fetching while still being respectful
TIME_BETWEEN_QUERIES = 0.1


def fetch_all_data():
    """
    Fetch all World Mineral Statistics data from the BGS OGC API.

    Returns:
        List of all feature records from the API.
    """
    all_features = []
    offset = 0

    # First request to get the total number of records
    log.info("Fetching initial data to determine total records (may take a few minutes)...")
    params = {"f": "json", "limit": PAGE_LIMIT, "offset": offset}

    response = requests.get(API_BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    total_records = data.get("numberMatched", 0)
    log.info(f"Total records to fetch: {total_records:,}")

    # Add first batch of features
    all_features.extend(data.get("features", []))
    offset += len(data.get("features", []))

    # Fetch remaining pages with progress bar
    with tqdm(total=total_records, initial=offset, desc="Fetching records") as pbar:
        while offset < total_records:
            params = {"f": "json", "limit": PAGE_LIMIT, "offset": offset}

            response = requests.get(API_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            offset += len(features)
            pbar.update(len(features))

            # Wait before sending next query to be respectful to the server
            time.sleep(TIME_BETWEEN_QUERIES)

    log.info(f"Successfully fetched {len(all_features):,} records")
    return all_features


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    """
    Main function to fetch BGS World Mineral Statistics and create a snapshot.
    """
    # Create a new snapshot.
    snap = Snapshot(f"bgs/{SNAPSHOT_VERSION}/world_mineral_statistics.zip")

    # Fetch all data from the new OGC API
    log.info("Starting data fetch from BGS OGC API-Features endpoint...")
    all_features = fetch_all_data()

    # Convert the feature collection to a JSON structure
    # This maintains the GeoJSON FeatureCollection format
    json_data = json.dumps(
        {
            "type": "FeatureCollection",
            "features": all_features,
            "numberMatched": len(all_features),
            "numberReturned": len(all_features),
        },
        indent=2,
    )

    # Ensure output snapshot folder exists, otherwise create it.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Create a zip file and add the JSON data to it
    log.info(f"Creating zip file at {snap.path}...")
    with zipfile.ZipFile(snap.path, "w", zipfile.ZIP_DEFLATED) as zipf:
        # Add the JSON data to the zip file.
        zipf.writestr(f"{snap.metadata.short_name}.json", json_data)

    log.info(f"Zip file created successfully with {len(all_features):,} records")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    run()
