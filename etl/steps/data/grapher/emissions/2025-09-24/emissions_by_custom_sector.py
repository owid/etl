"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("emissions_by_custom_sector")

    # Read table from garden dataset.
    tb = ds_garden.read("emissions_by_custom_sector")

    #
    # Prepare data.
    #
    # Adapt to grapher format.
    tb = tb.drop(columns=["country"], errors="raise").rename(columns={"sector": "country"}, errors="raise")

    # Improve format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
