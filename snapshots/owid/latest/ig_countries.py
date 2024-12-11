"""Script to create a snapshot of dataset.

File lives in GD: https://docs.google.com/spreadsheets/d/1SY7K_hyMtJUhyXDtQQwXAHgSNl22vrOqR6e3RljEU9I/edit?gid=917952968#gid=917952968

Example execution:

    python snapshots/owid/latest/ig_countries.py --path-to-file snapshots/owid/latest/countries.csv
    etlr ig_countries --private
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
    snap = Snapshot(f"owid/{SNAPSHOT_VERSION}/ig_countries.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
