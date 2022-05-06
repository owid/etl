from typing import Iterable

from owid import catalog

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh

NAMESPACE = "faostat"
VERSION = "2022-04-26"
DATASET_SHORT_NAME = "faostat_rl"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(
        DATA_DIR / "garden" / NAMESPACE / VERSION / DATASET_SHORT_NAME
    )
    # short_name should include dataset name and version
    dataset.metadata.short_name = f"{DATASET_SHORT_NAME}__{VERSION}".replace("-", "_")

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset[DATASET_SHORT_NAME].reset_index()

    table["entity_id"] = gh.country_to_entity_id(table["country"])

    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    yield from gh.yield_wide_table(table, na_action="drop")
