"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_constant = paths.load_snapshot("global_military_spending_dataset.rds")
    snap_burden = paths.load_snapshot("global_military_spending_dataset_burden.rds")

    # Load data from snapshot.
    tb_constant = snap_constant.read()
    tb_burden = snap_burden.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_constant = tb_constant.format(["gwno", "year", "indicator"])
    tb_burden = tb_burden.format(["ccode", "year", "indicator"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[
            tb_constant,
            tb_burden,
        ],
        check_variables_metadata=True,
        default_metadata=snap_constant.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
