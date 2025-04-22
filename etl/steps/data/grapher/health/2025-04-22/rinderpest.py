"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("rinderpest")

    # Read table from garden dataset.
    tb = ds_garden.read("rinderpest", reset_index=True)
    # A year is necessary for grapher, so we add a dummy year column.
    tb["year"] = 0

    #
    # Save outputs.
    tb = tb.format(["country", "year"])
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
