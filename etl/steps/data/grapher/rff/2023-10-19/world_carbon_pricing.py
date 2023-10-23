"""Load a garden dataset and create a grapher dataset."""
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read the table on carbon pricing on any sector.
    ds_garden = paths.load_dataset("world_carbon_pricing")
    tb_garden = ds_garden["world_carbon_pricing_any_sector"]

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
