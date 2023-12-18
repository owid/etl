# TODO: This file is a duplicate of the previous step. It is not yet used in the dag and should be updated soon.

"""Load garden dataset of photovoltaic cost and capacity and create a grapher dataset.

"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load table from garden dataset.
    ds_garden: Dataset = paths.load_dependency("photovoltaic_cost_and_capacity")
    tb_garden = ds_garden["photovoltaic_cost_and_capacity"]

    # Remove unnecessary columns.
    tb_garden = tb_garden.drop(columns=["cost_source", "cumulative_capacity_source"])

    # Create a new grapher dataset.
    dataset = create_dataset(dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)
    dataset.save()
