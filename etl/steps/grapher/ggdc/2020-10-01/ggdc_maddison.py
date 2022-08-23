from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def get_grapher_dataset() -> catalog.Dataset:
    dataset = N.garden_dataset
    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)

    # backward compatibility
    dataset.metadata.short_name = "ggdc_maddison__2020_10_01"

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["maddison_gdp"].reset_index()

    table = gh.adapt_table_for_grapher(table[["country", "year", "gdp", "gdp_per_capita", "population"]])

    yield from gh.yield_wide_table(table, na_action="drop")
