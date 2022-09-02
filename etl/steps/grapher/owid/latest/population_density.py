from collections.abc import Iterable

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden/owid/latest/population_density")
    dataset.metadata = gh.combine_metadata_sources(metadata=dataset.metadata)

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    table = dataset["population_density"].reset_index()

    table = gh.adapt_table_for_grapher(table)

    __import__("ipdb").set_trace()

    yield from gh.yield_wide_table(table)
