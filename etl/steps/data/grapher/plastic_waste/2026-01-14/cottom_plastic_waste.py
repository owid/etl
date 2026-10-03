"""Grapher step for plastic waste data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("cottom_plastic_waste")

    # Read tables from garden dataset.
    tb = ds_garden.read("cottom_plastic_waste", reset_index=False)

    #
    # Save outputs.
    #
    # Create grapher dataset with both tables.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata)

    ds_grapher.save()
