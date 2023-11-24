"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve all snapshots of the dataset.
    snap_annual = paths.load_snapshot("national_contributions_annual_emissions.csv")
    snap_cumulative = paths.load_snapshot("national_contributions_cumulative_emissions.csv")
    snap_temperature = paths.load_snapshot("national_contributions_temperature_response.csv")

    # Load data from snapshots.
    tb_annual = snap_annual.read(underscore=True)
    tb_cumulative = snap_cumulative.read(underscore=True)
    tb_temperature = snap_temperature.read(underscore=True)

    #
    # Process data.
    #
    # Combine all data into one table.
    tb = pr.concat(
        [
            tb_annual.assign(**{"file": "annual_emissions"}),
            tb_cumulative.assign(**{"file": "cumulative_emissions"}),
            tb_temperature.assign(**{"file": "temperature_response"}),
        ],
        ignore_index=True,
        short_name=paths.short_name,
    )

    # Set an appropriate index and sort conveniently.
    tb = (
        tb.rename(columns={"cntr_name": "country"}, errors="raise")
        .set_index(["country", "year", "file", "gas", "component"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as one of the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
