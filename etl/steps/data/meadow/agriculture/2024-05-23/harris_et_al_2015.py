"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("harris_et_al_2015.csv")
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Rename columns.
    tb = tb.rename(columns={"Years": "year", "Source": "source", "Total": "daily_calories"}, errors="raise")

    # Add a country column.
    tb["country"] = "England and Wales"

    # Format table conveniently.
    tb = tb.format(["country", "year", "source"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
