"""Load garden dataset of photovoltaic cost and capacity and create a grapher dataset.

"""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load table from garden dataset.
    ds_garden = paths.load_dataset("photovoltaic_cost_and_capacity")
    tb_garden = ds_garden["photovoltaic_cost_and_capacity"]

    # Remove unnecessary columns.
    tb_garden = tb_garden.drop(columns=["cost_source", "cumulative_capacity_source"], errors="raise")

    # Create a new grapher dataset.
    dataset = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    dataset.save()
