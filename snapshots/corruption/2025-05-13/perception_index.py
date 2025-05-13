"""Script to create a snapshot of the dataset.

The original XLSX file is impossible to open, so instead of directly downloading it here,
the file was manually downloaded, and the sheet of interest ('CPI2024-Results-and-trends')
was extracted and saved as a CSV file.
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-file", "-f", prompt=True, type=str, help="Path to local data file.")
def run(path_to_file: str, upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"corruption/{SNAPSHOT_VERSION}/perception_index.csv")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
