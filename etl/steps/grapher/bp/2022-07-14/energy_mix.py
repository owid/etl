"""Grapher step for BP's energy mix 2022 dataset.

"""
from typing import Iterable

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from owid import catalog
from . import NAMESPACE, VERSION


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden" / NAMESPACE / VERSION / "energy_mix")
    assert len(dataset.metadata.sources) == 1

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    # There is only one table in the dataset, with the same name as the dataset.
    table = dataset[dataset.table_names[0]].reset_index()

    # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
    table["entity_id"] = gh.country_to_entity_id(table["country"])
    table = table.drop(columns=["country", "country_code"])
    table = table.set_index(["entity_id", "year"])

    yield from gh.yield_wide_table(table, na_action="drop")
