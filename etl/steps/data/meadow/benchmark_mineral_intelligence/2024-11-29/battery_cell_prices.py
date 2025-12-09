"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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
    ds_meadow = paths.create_dataset(tables=[tb, tb_by_chemistry], default_metadata=snap.metadata)
    ds_meadow.save()
