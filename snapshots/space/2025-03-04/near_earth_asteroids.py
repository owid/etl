"""Script to create a snapshot of dataset.

To get the data file:
* Go to: https://cneos.jpl.nasa.gov/stats/totals.html
* Select "Tab: Cumulative Totals"
* Scroll down and click "CSV" under the table to download data file.

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
    # Initialize a new snapshot.
    snap = Snapshot(f"space/{SNAPSHOT_VERSION}/near_earth_asteroids.csv")

    # Save snapshot.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    main()
