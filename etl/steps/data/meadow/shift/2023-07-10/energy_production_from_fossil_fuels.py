"""Load a snapshot and create a meadow dataset."""

from typing import cast

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("energy_production_from_fossil_fuels.csv"))

    # Load data from snapshot.
    tb = pr.read_csv(snap.path, metadata=snap.to_table_metadata(), underscore=True)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)
    ds_meadow.save()
