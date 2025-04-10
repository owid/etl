"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot(short_name="happiness")

    tb = snap.read_excel()

    tb = tb.rename(columns={"Country name": "country"})

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])

    # Save meadow dataset.
    ds_meadow.save()
