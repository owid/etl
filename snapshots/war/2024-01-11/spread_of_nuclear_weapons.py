"""Script to create a snapshot of dataset.

This snapshot will fetch a PDF document. The data is manually extracted from a table in the PDF at the meadow step.
NOTE: As the first page of the document says, "This is intended to be a living document, and will be updated
periodically, perhaps on an annual basis, as new information emerges."
It is unclear where different versions will be hosted, but possibly at the following link one can see the latest
publication date (currently, June 13, 2017):
https://nonproliferation.org/when-did-and-didnt-states-proliferate/

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"war/{SNAPSHOT_VERSION}/spread_of_nuclear_weapons.csv")

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
