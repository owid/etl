"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    snap = paths.load_snapshot()
    tb = snap.read(safe_types=False)
    # The dataset has a variable named "Year" (the actual year of observation) that collides
    # with the lowercase "year" index column (all zeros in this legacy dataset). Rename it.
    tb = tb.rename(columns={"Year": "year_value"})
    tb = tb.set_index(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
