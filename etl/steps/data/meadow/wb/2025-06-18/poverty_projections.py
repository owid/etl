"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_projections = paths.load_snapshot("poverty_projections.dta")
    snap_aggregates = paths.load_snapshot("poverty_aggregates.dta")

    # Load data from snapshot.
    tb_projections = snap_projections.read()
    tb_aggregates = snap_aggregates.read()

    #
    # Process data.
    #
    # Improve tables format.
    tables = [
        tb_projections.format(["region_code", "year", "poverty_line"]),
        tb_aggregates.format(["region_code", "year", "poverty_line"]),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_projections.metadata)

    # Save meadow dataset.
    ds_meadow.save()
