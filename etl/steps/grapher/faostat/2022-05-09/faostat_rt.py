from typing import Iterable
from .shared import catalog, get_grapher_dataset_from_file_name, get_grapher_tables as _get_grapher_tables  # noqa:F401

import etl.grapher_helpers as gh


def get_grapher_dataset() -> catalog.Dataset:
    return get_grapher_dataset_from_file_name(__file__)


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    # By construction there should only be one table in each dataset. Load that table.
    assert len(dataset.table_names) == 1
    table_name = dataset.table_names[0]
    table = dataset[table_name].reset_index()

    # Convert country names into grapher entity ids, and set index appropriately.
    table["entity_id"] = gh.country_to_entity_id(table["country"])
    table = table.set_index(["entity_id", "year"]).drop(columns=["country"])

    # TODO: There is an issue when upserting variables into grapher that start with a number. Remove this temporary
    #  solution (and use the default shared.get_grapher_dataset) once the issue is solved.
    # Remove columns that start with a numbers, which cause issues when inserting into grapher.
    table = table.drop(columns=[column for column in table.columns if column.startswith('_')])

    yield from gh.yield_wide_table(table, na_action="drop")
