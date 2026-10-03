"""The data from the EIU is not shared in a machine-readable format. Instead, the EIU shares a single-year PDF report every year.

To overcome this we snapshot yearly reports (see snapshots/democracy/2024-05-22/eiu_dem_index.py for more details).

All these trancriptions and imports are saved in a Google sheet: https://docs.google.com/spreadsheets/d/1902iwPdR-PKjmpONceb1u9h2GzR-9Kzac4C9cnNDcHo/edit?usp=sharing.
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
    snap = Snapshot(f"democracy/{SNAPSHOT_VERSION}/eiu_2024.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
