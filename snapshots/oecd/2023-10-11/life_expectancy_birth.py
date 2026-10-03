"""The file is found in the data site from OECD:

https://data.oecd.org/healthstat/life-expectancy-at-birth.htm

In order to find the direct link to download the CSV:

    - Monitor the network traffic from the web developer inspect tools.
    - Click on download -> Full indicator data (.csv)
    - In the network traffic tab you will observe an API request with the direct CSV download link. This is the link used in url_download in the yaml file."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/life_expectancy_birth.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
