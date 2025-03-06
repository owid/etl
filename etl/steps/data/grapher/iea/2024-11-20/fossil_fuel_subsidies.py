"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("fossil_fuel_subsidies")

    # Read table from garden dataset.
    tb = ds_garden.read("fossil_fuel_subsidies", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save changes in the new grapher dataset.
    ds_grapher.save()
