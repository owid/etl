"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("fogel_2004.csv")
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Transform data to have a year column.
    tb = tb.melt(id_vars=["Year"], var_name="country", value_name="daily_calories")

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
