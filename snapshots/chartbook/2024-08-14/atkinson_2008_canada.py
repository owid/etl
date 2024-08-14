"""
Script to create a snapshot of dataset.

The file comes from Appendix C in the original book, available to buy here: https://academic.oup.com/book/38708.
I use a xls file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1zxuxAXriOrp0x_dxklVbULKGqG3fEOGBkouUNWx7AY8/edit?gid=1521965312#gid=1521965312
After creating the file, run
    python snapshots/chartbook/2024-08-05/atkinson_2008_canada.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/atkinson_2008_canada.xls")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
