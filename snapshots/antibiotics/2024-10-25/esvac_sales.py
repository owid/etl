"""
This dataset is manually downloaded. To recreate it follow these steps:

* Go here: https://esvacbi.ema.europa.eu/analytics/saw.dll?Dashboard
* Go to the 'Overall Sales' tab
* Select a year between 2010 and the latest available year.
* Click on the menu icon in the top right corner, just to the left of the ? icon
* Click on 'Export to Excel'
* Click on 'Export entire dashboard'
* Repeat this for each year, naming the downloaded file with the year
* Zip all files together and upload the zip file

### Beware that from 2017 onwards the UK row is pushed onto the next page of the table and must be downloaded or added separately.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"antibiotics/{SNAPSHOT_VERSION}/esvac_sales.zip")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
