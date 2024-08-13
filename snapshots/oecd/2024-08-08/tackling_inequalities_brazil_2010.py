"""
Script to create a snapshot of dataset.

The file comes from the original OECD publication, available here: https://www.oecd-ilibrary.org/social-issues-migration-health/tackling-inequalities-in-brazil-china-india-and-south-africa-2010_9789264088368-en.
I use a csv file coying the data from Table 2.3 (page 67), keeping only 'date' and the last column ('gini')
After creating the file, run
    python snapshots/oecd/2024-08-08/tackling_inequalities_brazil_2010.py --path-to-file <path-to-file>
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
    snap = Snapshot(f"oecd/{SNAPSHOT_VERSION}/tackling_inequalities_brazil_2010.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
