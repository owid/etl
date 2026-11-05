"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Step 1: Load the garden table.
    ds_garden = paths.load_dataset("fish_catch")
    tb = ds_garden.read("fish_catch", reset_index=False)

    # Step 2: Save the grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
