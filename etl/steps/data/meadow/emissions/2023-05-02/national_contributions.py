"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("national_contributions.start")

    #
    # Load inputs.
    #
    # Retrieve all snapshots of the dataset.
    snap_annual: Snapshot = paths.load_dependency("national_contributions_annual_emissions.csv")
    snap_cumulative: Snapshot = paths.load_dependency("national_contributions_cumulative_emissions.csv")
    snap_temperature: Snapshot = paths.load_dependency("national_contributions_temperature_response.csv")

    # Load data from snapshots.
    df_annual = pd.read_csv(snap_annual.path)
    df_cumulative = pd.read_csv(snap_cumulative.path)
    df_temperature = pd.read_csv(snap_temperature.path)

    #
    # Process data.
    #
    # Combine all data into one dataframe.
    combined = pd.concat(
        [
            df_annual.assign(**{"file": "annual_emissions"}),
            df_cumulative.assign(**{"file": "cumulative_emissions"}),
            df_temperature.assign(**{"file": "temperature_response"}),
        ],
        ignore_index=True,
    )

    # Create a table with the combined data.
    tb_meadow = Table(combined, short_name=paths.short_name, underscore=True)

    # Set an appropriate index and sort conveniently.
    tb_meadow = (
        tb_meadow.rename(columns={"cntr_name": "country"}, errors="raise")
        .set_index(["country", "year", "file", "gas", "component"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as one of the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_meadow], default_metadata=snap_annual.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("national_contributions.end")
