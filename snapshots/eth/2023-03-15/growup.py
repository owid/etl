"""
Script to create a snapshot of dataset 'Geographical Research On War, Unified Platform - GROW<sup>up</sup> (ETH, 2021)'.
The data is uploaded manually. To get the most recent data follow these steps:
    1. Go to https://growup.ethz.ch/rfe
    2. Select the level of aggregation "Country-Level Data".
    3. Select each of the categories (currently Power Access Data, Conflict Data, Settlement Area Data and Raster Aggregated Data)
    4. Press the Login/Register button.
    5. Enter your e-mail address
    6. If you are not registered you need an activation code sent to the email.
    7. Press "Download CSV".
    8. Extract the "data.csv" file from the zip
    9. Copy the file to this folder
    10. Upload the dataset by running (current version)
        python snapshots/eth/2023-03-15/growup.py --path-to-file snapshots/eth/2023-03-15/data.csv
    11. Delete the "data.csv" file from this folder

"""

from pathlib import Path

import click

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
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"eth/{SNAPSHOT_VERSION}/growup.csv")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
