"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_2003_2023 = paths.load_snapshot("weekly_wildfires_2003_2023.csv")
    snap_latest = paths.load_snapshot("weekly_wildfires.csv")

    # Load data from snapshot.
    tb_2003_2023 = snap_2003_2023.read()
    tb_latest = snap_latest.read()

    #
    # Process data.
    #
    tb = pr.concat([tb_latest, tb_2003_2023])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "month_day", "year", "indicator"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_latest.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
