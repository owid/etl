from owid import catalog
from shared import CURRENT_DIR

from etl.helpers import Names

GARDEN_DATASET_NAME = "emissions_weighted_carbon_price"
N = Names(str(CURRENT_DIR / GARDEN_DATASET_NAME))


def run(dest_dir: str) -> None:
    # Create new grapher dataset with the metadata from the garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Load table from garden dataset.
    table = N.garden_dataset[GARDEN_DATASET_NAME].reset_index()
    # Add table to new grapher dataset.
    dataset.add(table)
    # Save dataset.
    dataset.save()
