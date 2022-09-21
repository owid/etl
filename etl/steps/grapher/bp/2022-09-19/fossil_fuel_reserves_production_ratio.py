from owid import catalog
from shared import CURRENT_DIR

from etl import grapher_helpers as gh
from etl.helpers import Names

DATASET_SHORT_NAME = "fossil_fuel_reserves_production_ratio"
N = Names(str(CURRENT_DIR / DATASET_SHORT_NAME))


def run(dest_dir: str) -> None:
    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata))
    # Prepare table for grapher.
    table = N.garden_dataset[DATASET_SHORT_NAME].reset_index()
    table = gh.adapt_table_for_grapher(table)
    # Add table and save dataset.
    dataset.add(table)
    dataset.save()
