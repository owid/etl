"""Script to create a snapshot of dataset 'Energy and AI (IEA, 2025)'.

INSTRUCTIONS FOR RUNNING THIS SNAPSHOT:

Since the IEA dataset requires authentication, you need to manually download the file first:

1. Visit: https://www.iea.org/data-and-statistics/data-product/energy-and-ai
2. Create an IEA account or log in if you already have one
3. Download the Excel file
4. Place the downloaded file at: snapshots/artificial_intelligence/2025-11-07/energy_ai_iea.xlsx
5. Run this script with --path-to-file option:
   python snapshots/artificial_intelligence/2025-11-07/energy_ai_iea.py --path-to-file <path_to_downloaded_file>

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
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/energy_ai_iea.xlsx")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
