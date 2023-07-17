"""Script to create a snapshot of dataset 'European Health Information Gateway (WHO, 2022)'."""

import json
from pathlib import Path

import click
import pandas as pd
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/autopsy.csv")

    r = requests.get("https://dw.euro.who.int/api/v3/export/HFA_545?format=csv")
    assert r.ok
    url = json.loads(r.content.decode("utf-8"))["download_url"]
    df = pd.read_csv(url, skiprows=25)
    # assert column names are as expected
    assert df.columns.to_list() == ["COUNTRY", "COUNTRY_GRP", "SEX", "YEAR", "VALUE"]
    # Download data from source.
    df_to_file(df, file_path=snap.path)
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
