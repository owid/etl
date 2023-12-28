"""Script to create a snapshot of dataset 'Energy Institute Statistical Review of World Energy (2023)'

NOTE: On next update, check if the Energy Institute has fixed the following issue:
(from https://github.com/owid/owid-issues/issues/1267)
Many zeros in the main (excel) file of the Statistical Review are missing in the "Consolidated dataset".
For example, the gas consumption in EJ in Norway between 1965 and 1976 is missing in their "Consolidated Dataset"
(and without gas, other derived indicators are also missing).
However, those years are informed (and zero) in their key report excel file.
The same issue happens to many other countries and indicators.
We use the consolidated dataset because it has a much more convenient format, but maybe we should use the key report
excel file and deal with its very inconvenient format.
We contacted the Energy Institute about this issue and are waiting for a response.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name
# Data files to snapshot.
SNAPSHOT_FILES = [
    "statistical_review_of_world_energy.csv",
    "statistical_review_of_world_energy.xlsx",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot.
    for snapshot_file in SNAPSHOT_FILES:
        snap = Snapshot(f"energy_institute/{SNAPSHOT_VERSION}/{snapshot_file}")

        # Download data from source and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
