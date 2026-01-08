"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("growth_rate_gdp_productivity")

    # Read table from garden dataset.
    tb_productivity = ds_garden.read("growth_rate_productivity", reset_index=False)
    tb_gdp_pc = ds_garden.read("growth_rate_gdp_pc", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_productivity, tb_gdp_pc], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
