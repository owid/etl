"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import owid.catalog.processing as pr
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"cancer/{SNAPSHOT_VERSION}/gco_infections.csv")
    base_url = snap.metadata.origin.url_download

    jsons = ["by-agent.json", "by-cancers.json", "all.json"]
    dataframes = []
    for json in jsons:
        # Fetch the JSON data from the URL
        response = requests.get(base_url + json)

        # Parse the JSON data
        data = response.json()
        # Convert JSON data to DataFrame
        df = pd.DataFrame(data)
        df["year"] = 2020

        if json == "by-agent.json":
            df["cancer"] = "All cancers but non-melanoma skin cancer (C00-97, but C44)"
        elif json == "by-cancers.json":
            df["agent"] = "All infectious agents"
        elif json == "all.json":
            df = df.rename(columns={"site": "cancer"})
        dataframes.append(df)
    all_dfs = pd.concat(dataframes, ignore_index=True)
    df_to_file(all_dfs, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
