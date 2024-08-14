"""
Script to create a snapshot of dataset.

The file comes from Table 11.11 in the original paper, available in this book https://www.cambridge.org/us/universitypress/subjects/economics/public-economics-and-public-policy/changing-patterns-distribution-economic-welfare-economic-perspective
Due to the paywall, I am creating a csv file from the data extracted in the past by the Chartbook team. See https://docs.google.com/spreadsheets/d/1sySmwtRs_MvRrcVj52o9b0nG-bzbPMu1haIoSK8IWqk/edit?gid=1521965312#gid=1521965312
After creating the file, run
    python snapshots/chartbook/2024-04-22/concialdi.py --path-to-file <path-to-file>

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
    snap = Snapshot(f"chartbook/{SNAPSHOT_VERSION}/concialdi.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
