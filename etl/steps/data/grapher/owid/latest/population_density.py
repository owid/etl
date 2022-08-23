import pandas as pd
from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(
        dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata)
    )
    # dataset.metadata = gh.combine_metadata_sources(metadata=dataset.metadata)
    dataset.save()

    orig_table = N.garden_dataset["population_density"].reset_index()

    # add dimension artificially
    table = pd.concat([orig_table.assign(sex="male"), orig_table.assign(sex="female")])
    table.metadata = orig_table.metadata
    for col in table.columns:
        if col != "sex":
            table[col].metadata = orig_table[col].metadata

    table = table.set_index("sex")

    table = gh.adapt_table_for_grapher(table)

    dataset.add(table)
