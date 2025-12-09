"""Script to create a snapshot of dataset.
Extracting this data was technically challenging using Python, as the source was a scanned book.
The workflow involved several manual and AI-assisted steps:
    1. Screenshot capture: Specific table data was located on pages 36 and 42 of the PDF, and screenshots of these pages were taken.
    2. AI-assisted extraction: Claude (Anthropic's AI model) was used to extract structured data from the screenshots.
    3. Manual formatting: The extracted content was reviewed, and saved as a CSV file manually.
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
    snap = Snapshot(f"education/{SNAPSHOT_VERSION}/literacy_1950.csv")

    # Save snapshots.
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()
