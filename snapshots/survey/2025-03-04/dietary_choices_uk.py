"""Script to create a snapshot of dataset.

NOTE: The date_published is assumed to be the latest date in the spreadsheet.
In the future, consider extracting this date programmatically (the name of the latest column in the "All adults" sheet).

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"survey/{SNAPSHOT_VERSION}/dietary_choices_uk.xlsx")

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
