"""Script to create a snapshot of dataset."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from owid.datautils.io import df_to_file
from tqdm import tqdm

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"wb/{SNAPSHOT_VERSION}/edstats.csv")

    df = pd.read_csv(snap.metadata.origin.url_download, low_memory=False)

    # Using ThreadPoolExecutor for parallel requests with progress bar
    indicators = df["INDICATOR"].unique()
    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Wrap the executor in tqdm to show progress
        futures = {executor.submit(fetch_indicator_data, indicator): indicator for indicator in indicators}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching indicators"):
            result = future.result()
            results.append(result)

    # Create a temporary DataFrame from the results
    temp_df = pd.DataFrame(results, columns=["INDICATOR", "source_note", "source", "unit"])

    # Merge the results back into the original DataFrame
    df = pd.merge(temp_df, df, on="INDICATOR", how="right")
    df_to_file(df, file_path=snap.path)
    # Download data from source, add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def fetch_indicator_data(indicator):
    # Fetch metadata for an indicator
    url = f"https://api.worldbank.org/v2/indicator/{indicator}?format=json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Check if the response contains the expected data structure
        if len(data) > 1 and isinstance(data[1], list) and len(data[1]) > 0:
            source_note = data[1][0].get("sourceNote", np.nan)
            source = data[1][0].get("sourceOrganization", np.nan)
            unit = data[1][0].get("unit", np.nan)
            return indicator, source_note, source, unit
        else:
            print(f"No metadata found for indicator: {indicator}")
            return indicator, np.nan, np.nan, np.nan
    else:
        print(f"Failed to fetch data for indicator: {indicator}. Status code: {response.status_code}")
        return indicator, np.nan, np.nan, np.nan


if __name__ == "__main__":
    main()
