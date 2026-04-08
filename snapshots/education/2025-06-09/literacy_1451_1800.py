"""Script to create a snapshot of dataset.
To download the dataset go to https://www.researchgate.net/publication/46544350_Charting_the_Rise_of_the_West_Manuscripts_and_Printed_Books_in_Europe_A_Long-Term_Perspective_from_the_Sixth_through_Eighteenth_Centuries
and download the PDF file."""

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
    snap = Snapshot(f"education/{SNAPSHOT_VERSION}/literacy_1451_1800.pdf")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
