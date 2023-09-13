"""Grapher step for the fossil fuel production dataset.
"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("fossil_fuel_production")

    # Read table from garden dataset.
    tb_garden = ds_garden["fossil_fuel_production"].reset_index()

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)
    ds_grapher.save()
