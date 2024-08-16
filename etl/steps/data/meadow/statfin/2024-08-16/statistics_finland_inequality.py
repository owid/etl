"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_gini = paths.load_snapshot("gini_coefficient.csv")
    snap_rel = paths.load_snapshot("relative_poverty.csv")
    snap_wealth = paths.load_snapshot("share_wealth.csv")

    # Load data from snapshot.
    tb_gini = snap_gini.read(na_values=["."])
    tb_rel = snap_rel.read(na_values=["."])
    tb_wealth = snap_wealth.read(na_values=["."])

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_gini = tb_gini.format(["year"])
    tb_rel = tb_rel.format(["year"])
    tb_wealth = tb_wealth.format(["year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_gini, tb_rel, tb_wealth],
        check_variables_metadata=True,
        default_metadata=snap_gini.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
