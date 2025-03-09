"""
Script to create a snapshot of dataset.

The file was extracted from the tables in Annex C - OECD social expenditure statistics in the book available in the Internet Archive:
https://archive.org/details/socialexpenditur0000unse

I uploaded the data available in the "OECD 1985 original" sheet in the Google Sheets:
https://docs.google.com/spreadsheets/d/112vwOK9WIAc0s-yfeLUvjhC-cVP1swPR/edit?gid=498003109#gid=498003109

That table didn't include the complete data for Finland and Sweden, so I added the data from the book.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/social_expenditure_1985.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
