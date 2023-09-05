"""Load a snapshot and create a meadow dataset."""

from typing import cast

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("world_bank_pip.csv"))

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb.set_index(
        ["ppp_version", "poverty_line", "country", "year", "reporting_level", "welfare_type"], verify_integrity=True
    ).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
