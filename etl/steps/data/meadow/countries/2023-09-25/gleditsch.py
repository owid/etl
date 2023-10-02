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
    snap_states = paths.load_snapshot("gleditsch_states.dat")
    snap_micro = paths.load_snapshot("gleditsch_microstates.dat")

    # Load data from snapshot.
    tb_states = snap_states.read_csv(
        delimiter="\t", encoding="unicode_escape", names=["id", "iso", "country", "start", "end"], index_col=False
    )
    tb_micro = snap_micro.read_csv(
        delimiter="\t", encoding="unicode_escape", names=["id", "iso", "country", "start", "end"], index_col=False
    )

    #
    # Process data.
    #
    # Combine tables
    tb = pr.concat([tb_states, tb_micro], ignore_index=True, short_name=paths.short_name)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["id", "start", "end"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_states.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
