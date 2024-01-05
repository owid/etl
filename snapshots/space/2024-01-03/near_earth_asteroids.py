"""Script to create a snapshot of dataset."""

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
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/near_earth_asteroids.csv")

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    # Source URL: https://cneos.jpl.nasa.gov/stats/totals.html
    # Tab: Cumulative Totals
    # Scroll down and click "CSV" under the table
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
