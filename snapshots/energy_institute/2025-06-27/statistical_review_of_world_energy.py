"""Script to create a snapshot of dataset 'Energy Institute Statistical Review of World Energy'

The Energy Institute offers the same data (although in practice they have differences) in different formats:
* Narrow-format (long-format) ~25MB csv file (also in xlsx format).
* Panel-format (wide-format) ~3MB csv file (also in xlsx format).
* Additionally, they provide the main data file, which is a human-readable xlsx file with many sheets.

Over the years, I have detected different issues with zeros and missing values in their "consolidated datasets" (from https://github.com/owid/owid-issues/issues/1267). Here are some examples:

- In the 2023 and 2024 releases, Norway's gas consumption (in EJ) was missing in the panel format file between 1965 and 1976 (while they were zero in the main excel file). This issues was fixed in the 2025 release (those years became zero).
- In the 2025 release, the narrow format file is affected by the same problem: Norway gascons_ej doesn't have data between 1965 and 1976.
- In the 2025 release, Norway and Total World (and probably other countries) have zero for solar_twh and wind_twh (and other related columns) for all years in the panel format file! The narrow-format file is not affected, though.
- The main excel file (which is much harder to parse programmatically) contains some variables that are not given in the other data files (e.g. data on coal reserves).
- Additionally, primary energy consumption is not included in the consolidated dataset (only in the main excel file).

I will contact Energy Institute about these issues.

In the mean time, the safest option seems to be to extract all data from the main excel file, which is the source of truth.

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
def run(upload: bool) -> None:
    # Create a new snapshot.
    for snapshot_file in SNAPSHOT_FILES:
        snap = Snapshot(f"energy_institute/{SNAPSHOT_VERSION}/{snapshot_file}")

        # Download data from source and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
