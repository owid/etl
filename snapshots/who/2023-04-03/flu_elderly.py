"""Script to create a snapshot of dataset 'Influenza Vaccination Coverage (WHO; UNICEF, 2023)'.

To access the data visit this url: https://immunizationdata.who.int/pages/coverage/flu.html?ANTIGEN=FLU_ELDERLY&YEAR=

Select 'All countries' and 'Influenza elderly' and widest range of years possible.

Click 'Apply' and then 'Download' to get the xlsx file locally.

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
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/flu_elderly.xlsx")

    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
