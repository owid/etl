from owid import catalog
from collections.abc import Iterable

from etl.paths import DATA_DIR
from etl import grapher_helpers as gh


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(
        DATA_DIR / "garden" / "ggdc" / "2020-10-01" / "ggdc_maddison"
    )
    # short_name should include dataset name and version
    dataset.metadata.short_name = "ggdc_maddison__2020_10_01"

    # move description to source as that is what is shown in grapher
    # (dataset.description would be displayed under `Internal notes` in the admin UI otherwise)
    dataset.metadata.sources[0].description = dataset.metadata.description
    dataset.metadata.description = ""

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["maddison_gdp"].reset_index()

    table["entity_id"] = gh.country_to_entity_id(table["country"], errors="warn")

    # unmapped countries
    # TODO: should be automatically added in upsert, but are they?
    # {'South and South-East Asia', 'Western Offshoots', 'Eastern Europe', 'Western Europe',
    # 'Latin America', 'Sub-Sahara Africa', 'East Asia', 'Middle East'}
    table.dropna(subset=["entity_id", "gdp"], inplace=True)

    table = table.set_index(["entity_id", "year"])[
        ["gdp", "gdp_per_capita", "population"]
    ]

    yield from gh.yield_wide_table(table)
