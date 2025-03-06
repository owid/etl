"""
Script to create a snapshot of dataset.

The file was extracted from Table 1D in the paper available in ScienceDirect:
https://www.sciencedirect.com/science/article/abs/pii/S0014498384710011

If you can't access the paper, you can download the file from the following link:
https://drive.google.com/file/d/1YFlIC-on7oWiDhgdETUD9PSVE-BTaySn/view

The file was saved as a csv file by using a screenshot and processing it via ChatGPT"
    `Can you convert this table into a csv file?`

I only corrected the values where "zero" was written as 0. "zero" is described as "public spending was zero, but we lack data on a key independent variable".

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
    snap = Snapshot(f"health_expenditure/{SNAPSHOT_VERSION}/lindert.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
