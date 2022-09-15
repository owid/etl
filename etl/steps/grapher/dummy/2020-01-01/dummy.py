from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata))

    table = N.garden_dataset["dummy"]

    # convert `country` into `entity_id`
    table = gh.adapt_table_for_grapher(table)

    # optionally set additional dimensions
    # table = table.set_index(["sex", "income_group"], append=True)

    # if you data is in long format, check gh.long_to_wide_tables
    dataset.add(table)

    dataset.save()
