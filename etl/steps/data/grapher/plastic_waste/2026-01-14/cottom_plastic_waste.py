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
    tb_national = ds_garden["cottom_plastic_waste_national"]
    tb_regional = ds_garden["cottom_plastic_waste_regional"]

    #
    # Save outputs.
    #
    # Create grapher dataset with both tables.
    ds_grapher = paths.create_dataset(
        tables=[tb_national, tb_regional],
        check_variables_metadata=True,
    )

    ds_grapher.save()
