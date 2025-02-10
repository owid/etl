"""Script to create a snapshot of dataset.

As of 2024-11-29, the WHO reports three files for cases & deaths:

- [NEW] Daily frequency reporting of new COVID-19 cases and deaths by date reported to WHO: Mostly weekly data, but occasionally daily data (especially past data).
- Weekly COVID-19 cases and deaths by date reported to WHO: Reports weekly values. This is what we have been using since we switched from JHU to WHO.
- Latest reported counts of COVID-19 cases and deaths: Reports latest values (only latest date is available)


"""

from datetime import date
from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"covid/{SNAPSHOT_VERSION}/cases_deaths.csv")

    # Update metadata
    snap = modify_metadata(snap)

    # Download data from source, add file to DVC and upload to S3.
    snap.create_snapshot(upload=upload)


def modify_metadata(snap: Snapshot) -> Snapshot:
    """Modify metadata"""
    # Get access date
    snap.metadata.origin.date_accessed = date.today()  # type: ignore
    # Set publication date
    snap.metadata.origin.date_published = date.today().year  # type: ignore
    # Save
    snap.metadata.save()
    return snap


if __name__ == "__main__":
    main()
