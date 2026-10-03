"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("cost_of_space_launches")

    # Read table from garden dataset.
    tb = ds_garden.read("cost_of_space_launches")

    #
    # Process data.
    #
    # Adapt column names to grapher.
    tb = tb.rename(columns={"vehicle": "country", "first_launch_year": "year"}, errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
