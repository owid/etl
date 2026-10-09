"""Load garden dataset of photovoltaic cost and capacity and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    # Load table from garden dataset.
    ds_garden = paths.load_dataset("photovoltaic_cost_and_capacity")
    tb_garden = ds_garden.read("photovoltaic_cost_and_capacity", reset_index=False)

    # Remove unnecessary columns.
    tb_garden = tb_garden.drop(columns=["cost_source", "cumulative_capacity_source"], errors="raise")

    # Create a new grapher dataset.
    dataset = paths.create_dataset(tables=[tb_garden])
    dataset.save()
