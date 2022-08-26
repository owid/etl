from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def get_grapher_dataset() -> catalog.Dataset:
    dataset = N.garden_dataset
    # combine sources into a single one and create proper names
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)
    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["{{cookiecutter.short_name}}"]

    # convert `country` into `entity_id` and set indexes for `yield_wide_table`
    table = gh.adapt_table_for_grapher(table)

    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # convert table into grapher format
    # if you data is in long format, use gh.yield_long_table
    yield from gh.yield_wide_table(table, na_action="drop")
