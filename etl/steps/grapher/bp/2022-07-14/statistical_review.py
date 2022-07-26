"""Grapher step for BP's statistical review 2022 dataset.

"""
from typing import Iterable

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR
from owid import catalog
from . import NAMESPACE, VERSION


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(
        DATA_DIR / "garden" / NAMESPACE / VERSION / "statistical_review"
    )
    assert len(dataset.metadata.sources) == 1

    # Add institution and year to dataset short name (the name that will be used in grapher database).
    dataset.metadata.short_name = dataset.metadata.short_name + f"__bp_2022"
    # Copy the dataset description to the source's description, since this is what is shown in grapher.
    dataset.metadata.sources[0].description = dataset.metadata.description
    # Empty dataset description (otherwise it will appear in `Internal notes` in the admin UI).
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    # There is only one table in the dataset, with the same name as the dataset.
    table = dataset[dataset.table_names[0]].reset_index()

    # Copy the new dataset description (which is now stored in sources) to the variable sources' description.
    data_columns = [
        col for col in table.columns if col not in ["year", "country", "country_code"]
    ]
    for col in data_columns:
        table[col].metadata.sources[0].description = dataset.metadata.sources[
            0
        ].description

    # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
    table["entity_id"] = gh.country_to_entity_id(table["country"])
    table = table.drop(columns=["country", "country_code"])
    table = table.set_index(["entity_id", "year"])

    yield from gh.yield_wide_table(table, na_action="drop")
