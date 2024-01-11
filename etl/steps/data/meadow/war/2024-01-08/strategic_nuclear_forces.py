"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots of dyadic data.
    snap = paths.load_snapshot("strategic_nuclear_forces.xlsx")
    tb_dyadic = snap.read()

    # Retrieve snapshots of monadic data.
    snap = paths.load_snapshot("strategic_nuclear_forces_monadic.xlsx")
    tb_monadic = snap.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_dyadic = tb_dyadic.underscore().set_index(["ccode1", "ccode2", "year"], verify_integrity=True).sort_index()
    tb_monadic = tb_monadic.underscore().set_index(["ccode", "year"], verify_integrity=True).sort_index()

    # Update table short names.
    tb_dyadic.metadata.short_name = "strategic_nuclear_forces_dyadic"
    tb_monadic.metadata.short_name = "strategic_nuclear_forces_monadic"

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb_dyadic, tb_monadic], check_variables_metadata=True)
    ds_meadow.save()
