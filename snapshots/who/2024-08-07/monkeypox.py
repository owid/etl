"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


WHO_REGIONS = ["EURO", "AMRO", "WPRO", "EMRO", "AFRO", "SEARO"]


def import_data() -> pd.DataFrame:
    df = pd.DataFrame()
    # Fetching the data for each WHO region separately
    for region in WHO_REGIONS:
        url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/MPX/V_MPX_VALIDATED_DAILY?&$format=csv&$filter=WHO_REGION%20eq%20%27{region}%27"
        df_region = pd.read_csv(url)
        df = pd.concat([df, df_region])
    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/monkeypox.csv")

    df = pd.DataFrame()
    # Fetching the data for each WHO region separately
    for region in WHO_REGIONS:
        url = f"https://frontdoor-l4uikgap6gz3m.azurefd.net/MPX/V_MPX_VALIDATED_DAILY?&$format=csv&$filter=WHO_REGION%20eq%20%27{region}%27"
        df_region = pd.read_csv(url)
        df = pd.concat([df, df_region])

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(data=df, upload=upload)


if __name__ == "__main__":
    main()
