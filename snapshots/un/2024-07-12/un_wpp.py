"""WPP 2024."""

from pathlib import Path

import click
from structlog import get_logger

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Log
log = get_logger()


########################################################################################################################
# TODO: Temporarily using a local file until 2024 revision is released
#  The download url should still be the same:
#  https://population.un.org/wpp
########################################################################################################################
@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snapshot_paths = [
        # Main
        # "un_wpp_demographic_indicators.xlsx",
        # Population
        # "un_wpp_population_estimates.csv",
        # "un_wpp_population_low.csv",
        # "un_wpp_population_medium.csv",
        # "un_wpp_population_high.csv",
        # "un_wpp_population_constant_fertility.csv",
        # Fertility
        "un_wpp_fertility_single_age.csv",
        # "un_wpp_fertility.csv",
        # Deaths
        # "un_wpp_deaths_estimates.csv",
        # "un_wpp_deaths_medium.csv",
    ]
    for paths in snapshot_paths:
        log.info(f"Importing {paths[1]}.")
        # Create a new snapshot.
        snap = Snapshot(f"un/{SNAPSHOT_VERSION}/{paths}")

        # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
