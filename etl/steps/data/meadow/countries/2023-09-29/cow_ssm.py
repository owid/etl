"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_majors = paths.load_snapshot("cow_ssm_majors.csv")
    snap_states = paths.load_snapshot("cow_ssm_states.csv")
    snap_system = paths.load_snapshot("cow_ssm_system.csv")

    # Load data from snapshot.
    tb_majors = snap_majors.read()
    tb_states = snap_states.read()
    tb_system = snap_system.read()

    #
    # Process data.
    #
    # Group tables, and format them
    tables = [
        tb_majors.underscore()
        .set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"], verify_integrity=True)
        .sort_index(),
        tb_states.underscore()
        .set_index(["ccode", "styear", "stmonth", "stday", "endyear", "endmonth", "endday"], verify_integrity=True)
        .sort_index(),
        tb_system.underscore().set_index(["ccode", "year"], verify_integrity=True).sort_index(),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap_states.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
