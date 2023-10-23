from owid import catalog
from shared import CURRENT_DIR

from etl.helpers import PathFinder

GARDEN_DATASET_NAME = "emissions_weighted_carbon_price"
GRAPHER_DATASET_TITLE = "Emissions-weighted carbon price (2022)"
N = PathFinder(str(CURRENT_DIR / GARDEN_DATASET_NAME))


def run(dest_dir: str) -> None:
    # Create new grapher dataset with the metadata from the garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.metadata.title = GRAPHER_DATASET_TITLE
    dataset.metadata.short_name = GARDEN_DATASET_NAME

    # Load table from garden dataset.
    table = N.garden_dataset[GARDEN_DATASET_NAME].reset_index()
    # Add table to new grapher dataset.
    dataset.add(table)
    # Save dataset.
    dataset.save()
