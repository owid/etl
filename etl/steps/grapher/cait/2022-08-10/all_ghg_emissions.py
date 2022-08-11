from typing import Iterable

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from owid import catalog

from .shared import NAMESPACE, GARDEN_DATASET_VERSION

# Name of table to load from garden dataset to convert into a grapher dataset.
TABLE_NAME = "greenhouse_gas_emissions_by_sector"
# Path to garden dataset to be loaded.
DATASET_PATH = DATA_DIR / "garden" / NAMESPACE / GARDEN_DATASET_VERSION / "ghg_emissions_by_sector"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATASET_PATH)
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    # There is only one table in the dataset, with the same name as the dataset.
    table = dataset[TABLE_NAME].reset_index().drop(columns=["population"])
    table = gh.adapt_table_for_grapher(table)

    yield from gh.yield_wide_table(table, na_action="drop")
