"""
INSTRUCTIONS FOR EXTRACTION
    1. Go to https://www.pewresearch.org/religion/fact-sheet/same-sex-marriage-around-the-world/
    2. Copy the table available in the website in Excel, for example. (Paste special>Paste special>Text)
    3. Add column titles for country and year and only keep those.
    4. Save as a CSV, called `same_sex_marriage.csv`.
    5. Place the CSV file in this directory.
    6. Run this script to create a snapshot of the dataset.
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
    snap = Snapshot(f"pew/{SNAPSHOT_VERSION}/same_sex_marriage.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
