from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

from .shared import CURRENT_DIR

N = Names(str(CURRENT_DIR / "world_carbon_pricing"))
TABLE_NAME = "world_carbon_pricing_any_sector"
# GRAPHER_DATASET_NAME = "World carbon pricing for any sector"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = N.garden_dataset
    # dataset.metadata.title = GRAPHER_DATASET_NAME
    # combine sources into a single one and create proper names
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset[TABLE_NAME].reset_index()
    table = gh.adapt_table_for_grapher(table)

    yield from gh.yield_wide_table(table, na_action="drop")
