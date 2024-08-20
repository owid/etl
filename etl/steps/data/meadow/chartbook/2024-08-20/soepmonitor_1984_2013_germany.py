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
    snap_gini = paths.load_snapshot("soepmonitor_1984_2013_germany_gini.csv")
    snap_relative_poverty = paths.load_snapshot("soepmonitor_1984_2013_germany_relative_poverty.csv")

    # Load data from snapshot.
    tb_gini = snap_gini.read()
    tb_relative_poverty = snap_relative_poverty.read()

    #
    # Process data.
    #
    # Merge tables.
    tb = pr.merge(tb_gini, tb_relative_poverty, on=["country", "year"], how="outer")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_gini.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
