"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("global_hen_inventory")
    tb = ds_garden.read("global_hen_inventory", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save the new grapher dataset.
    ds_grapher.save()
