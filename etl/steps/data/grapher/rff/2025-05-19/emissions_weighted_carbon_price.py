"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read the table on carbon pricing on any sector.
    ds_garden = paths.load_dataset("emissions_weighted_carbon_price")
    tb_garden = ds_garden.read("emissions_weighted_carbon_price", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_garden], default_metadata=ds_garden.metadata)
    ds_grapher.save()
