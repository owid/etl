from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

DATASET_PATH = DATA_DIR / "garden" / "rff" / "2022-09-14" / "world_carbon_pricing"
GRAPHER_DATASET_TITLE = "World carbon pricing for any sector"
TABLE_NAME = "world_carbon_pricing_any_sector"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_PATH)
    garden_dataset.metadata.title = GRAPHER_DATASET_TITLE
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))
    table = garden_dataset[TABLE_NAME].reset_index()
    dataset.add(gh.adapt_table_for_grapher(table))
