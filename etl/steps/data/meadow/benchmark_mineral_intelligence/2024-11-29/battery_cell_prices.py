"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = paths.load_snapshot("battery_cell_prices.xlsx")
    snap_by_chemistry = paths.load_snapshot("battery_cell_prices_by_chemistry.xlsx")

    # Load data from snapshots.
    tb = snap.read(skiprows=8)
    tb_by_chemistry = snap_by_chemistry.read(skiprows=7)

    #
    # Process data.
    #
    # Remove empty columns.
    tb = tb.dropna(axis=1, how="all")
    tb_by_chemistry = tb_by_chemistry.dropna(axis=1, how="all")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["year"])
    tb_by_chemistry = tb_by_chemistry.format(["date"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb, tb_by_chemistry], check_variables_metadata=True, default_metadata=snap.metadata
    )
    ds_meadow.save()
