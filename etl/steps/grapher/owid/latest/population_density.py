from owid import catalog
from collections.abc import Iterable

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden/owid/latest/population_density")
    dataset.metadata.sources = [gh.join_sources(dataset.metadata.sources)]

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset[
        "population_density"
    ].metadata.description

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["population_density"].reset_index()

    table = (
        table.assign(entity_id=gh.country_to_entity_id(table["country"])).set_index(
            ["entity_id", "year"],
        )
    )[["population_density"]]

    yield from gh.yield_wide_table(table)
