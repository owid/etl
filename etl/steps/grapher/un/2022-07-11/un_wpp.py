from typing import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp")
    assert len(dataset.metadata.sources) == 1

    # short_name should include dataset name and version
    dataset.metadata.short_name = "un_wpp__2022-07-11"

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["long"].reset_index()

    # grapher needs a column entity id, that is constructed based on the unique entity names in the database
    table["entity_id"] = gh.country_to_entity_id(table["location"])

    # use entity_id and year as indexes in grapher
    table = table.set_index(["entity_id", "year"])

    # convert table into grapher format
    # if you data is in long format, use gh.yield_long_table
    yield from gh.yield_wide_table(table, na_action="drop")
