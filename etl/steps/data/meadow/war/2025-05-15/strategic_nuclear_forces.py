"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots of dyadic data.
    snap = paths.load_snapshot("strategic_nuclear_forces__dyadic.xlsx")
    tb_dyadic = snap.read(safe_types=False)

    # Retrieve snapshots of monadic data.
    snap = paths.load_snapshot("strategic_nuclear_forces__monadic.xlsx")
    tb_monadic = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb_dyadic = tb_dyadic.format(["ccode1", "ccode2", "year"], short_name="strategic_nuclear_forces_dyadic")
    tb_monadic = tb_monadic.format(["ccode", "year"], short_name="strategic_nuclear_forces_monadic")

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_dyadic, tb_monadic])
    ds_meadow.save()
