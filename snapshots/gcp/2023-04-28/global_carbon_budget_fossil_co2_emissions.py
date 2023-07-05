"""Script to create snapshots of dataset related of the Global Carbon Budget.

A snapshot will be created for each of the following datasets:
* Global Carbon Budget - Fossil CO2 emissions.
* Global Carbon Budget - Global emissions.
* Global Carbon Budget - Land-use change emissions.
* Global Carbon Budget - National emissions.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of input data files to create snapshots for.
DATA_FILES = [
    "global_carbon_budget_fossil_co2_emissions.csv",
    "global_carbon_budget_global_emissions.xlsx",
    "global_carbon_budget_land_use_change_emissions.xlsx",
    "global_carbon_budget_national_emissions.xlsx",
]


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot for each dataset.
    for data_file in DATA_FILES:
        snap = Snapshot(f"gcp/{SNAPSHOT_VERSION}/{data_file}")

        # Download data from source.
        snap.download_from_source()

        # Add file to DVC and upload to S3.
        snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()
