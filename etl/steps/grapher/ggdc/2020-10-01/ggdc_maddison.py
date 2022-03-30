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

    # TODO: this is really slow, create a separate batch query and nice helper in grapher_helpers
    # get the remaining entities directly from the DB
    ix = table.entity_id.isnull()
    country_entity_id_map = {
        country: gh.get_or_create_entity(country)
        for country in set(table.loc[ix, "country"])
    }
    table.loc[ix, "entity_id"] = table.loc[ix, "country"].map(country_entity_id_map)
    assert table.entity_id.notnull().all()

    table.dropna(subset=["gdp"], inplace=True)

    table = table.set_index(["entity_id", "year"])[
        ["gdp", "gdp_per_capita", "population"]
    ]

    yield from gh.yield_wide_table(table)
