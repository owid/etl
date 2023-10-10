"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(
    dest_dir: str,
) -> None:
    # Load snapshot.
    snap = paths.load_snapshot("cause_hierarchy.csv")

    # Load data from snapshot.
    tb = snap.read(underscore=True)
    tb = tb.set_index(["cause_name"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
