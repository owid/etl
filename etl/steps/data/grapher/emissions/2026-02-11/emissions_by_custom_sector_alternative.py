"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("emissions_by_custom_sector_alternative")
    tb = ds_garden.read("emissions_by_custom_sector_alternative")

    #
    # Prepare data.
    #
    # Adapt to grapher format.
    tb = tb.drop(columns=["country"]).rename(columns={"sector": "country"}, errors="raise")

    # Improve format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save changes in the new grapher dataset.
    ds_grapher.save()
