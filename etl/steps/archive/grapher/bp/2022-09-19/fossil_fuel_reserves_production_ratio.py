from owid import catalog
from shared import CURRENT_DIR

from etl.helpers import PathFinder

DATASET_SHORT_NAME = "fossil_fuel_reserves_production_ratio"
N = PathFinder(str(CURRENT_DIR / DATASET_SHORT_NAME))


def run(dest_dir: str) -> None:
    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Prepare table for grapher.
    table = N.garden_dataset[DATASET_SHORT_NAME].reset_index()
    # Add table and save dataset.
    dataset.add(table)
    dataset.save()
