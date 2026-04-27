"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("monthly_burned_area")

    # Read table from garden dataset.
    tb = ds_garden["monthly_burned_area"]
    #
    # Process data.
    #
    # Data is already aggregated by country and year in garden.

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)
    ds_grapher.save()
