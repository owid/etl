"""Load garden dataset of photovoltaic cost and capacity and create a grapher dataset.

"""

from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load table from garden dataset.
    ds_garden: catalog.Dataset = paths.load_dependency("photovoltaic_cost_and_capacity")
    tb_garden = ds_garden["photovoltaic_cost_and_capacity"]

    # Remove unnecessary columns.
    tb_garden = tb_garden.drop(columns=["cost_source", "cumulative_capacity_source"])

    # Create a new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table to dataset and save dataset.
    dataset.add(tb_garden)
    dataset.save()
