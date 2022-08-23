from collections.abc import Iterable

import pandas as pd
from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR


def get_grapher_dataset() -> catalog.Dataset:
    dataset = catalog.Dataset(DATA_DIR / "garden/owid/latest/population_density")
    dataset.metadata = gh.combine_metadata_sources(metadata=dataset.metadata)

    return dataset


def get_grapher_tables(dataset: catalog.Dataset) -> Iterable[catalog.Table]:
    orig_table = dataset["population_density"].reset_index()

    # add dimension artificially
    table = pd.concat([orig_table.assign(sex="male"), orig_table.assign(sex="female")])
    table.metadata = orig_table.metadata
    for col in table.columns:
        if col != "sex":
            table[col].metadata = orig_table[col].metadata

    table = table.set_index("sex")

    table = gh.adapt_table_for_grapher(table)

    yield from gh.yield_wide_table(table)
