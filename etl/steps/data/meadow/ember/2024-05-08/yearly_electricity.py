"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table."""

from etl.helpers import PathFinder

# Get naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    snap = paths.load_snapshot("yearly_electricity.csv")
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Format table conveniently.
    tb = tb.format(keys=["area", "year", "variable", "unit"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
