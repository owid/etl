"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("decoupling_food_supply_and_land_use")
    tb = ds_garden.read("decoupling_food_supply_and_land_use", reset_index=False)

    #
    # Process data.
    #
    # Keep only needed columns.
    tb = tb[["food_energy", "agricultural_land"]]

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
