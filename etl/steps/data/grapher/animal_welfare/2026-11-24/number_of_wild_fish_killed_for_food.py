"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("number_of_wild_fish_killed_for_food")
    tb = ds_garden.read("number_of_wild_fish_killed_for_food", reset_index=False)
    tb_fishmeal = ds_garden.read("number_of_wild_fish_caught_for_fishmeal_and_fish_oil", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb, tb_fishmeal], default_metadata=ds_garden.metadata)
    ds_grapher.save()
