"""Script to create a snapshot of dataset.
    Download full data set from here: https://platform.who.int/mortality/themes/theme-details/topics/indicator-groups/indicator-group-details/MDB/violence
    Click on the download button and then select 'full dataset'. It doesn't matter what age groups or years you select it will download them all anyway.
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
    snap = Snapshot(f"homicide/{SNAPSHOT_VERSION}/who_mort_db.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
