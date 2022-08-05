"""Grapher step for the Electricity Mix (BP & Ember, 2022) dataset.

"""

from typing import Iterable

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from owid import catalog

# Path to garden dataset to be loaded.
DATASET_PATH = DATA_DIR / "garden" / "energy" / "2022-08-03" / "electricity_mix"


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATASET_PATH)
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    # There is only one table in the dataset, with the same name as the dataset.
    table = dataset[dataset.table_names[0]].reset_index().drop(columns=["population"])
    table = gh.adapt_table_for_grapher(table)

    yield from gh.yield_wide_table(table, na_action="drop")
