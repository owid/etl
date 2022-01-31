from owid import catalog
from collections.abc import Iterable

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


def get_grapher_dataset() -> catalog.Dataset:
    return catalog.Dataset(DATA_DIR / "garden/owid/latest/population_density")


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["population-density"].reset_index()

    table = (
        table.assign(entity_id=gh.country_to_entity_id(table["country"])).set_index(
            ["entity_id", "year"],
        )
    )[["population_density"]]

    yield from gh.yield_wide_table(table)
