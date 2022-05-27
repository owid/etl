"""Food explorer data step.

Loads the latest faostat_food_explorer dataset from garden and stores a table (as a csv file) for each food product.

NOTE: It will overwrite csv files inside "data/explorer/owid/latest/food_explorer".

"""
from copy import deepcopy

from owid import catalog
from tqdm.auto import tqdm

from etl.paths import DATA_DIR


def run(dest_dir: str) -> None:
    # Load the latest dataset for FAOSTAT food explorer from garden.
    dataset_garden_latest_dir = sorted(
        (DATA_DIR / "garden" / "faostat").glob("*/faostat_food_explorer")
    )[-1]
    dataset_garden = catalog.Dataset(dataset_garden_latest_dir)
    # Get the table of all food products.
    table_garden = dataset_garden["all_products"]

    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add dataset metadata.
    dataset.metadata = deepcopy(dataset_garden.metadata)
    dataset.metadata.namespace = "owid"
    dataset.metadata.short_name = "food_explorer"
    # Create new dataset in garden.
    dataset.save()

    # List all products in table
    products = sorted(table_garden.index.get_level_values("product").unique().tolist())
    for product in tqdm(products):
        # Save a table (as a separate csv file) for each food product.
        table_product = table_garden.loc[product]
        # Update table metadata.
        table_product.title = product
        table_product.metadata.short_name = catalog.utils.underscore(
            name=product, validate=True
        )
        # Add table to dataset.
        dataset.add(table_product, format="csv")
