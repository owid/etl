from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata))
    dataset.save()

    table = N.garden_dataset["maddison_gdp"].reset_index()

    table = gh.adapt_table_for_grapher(table[["country", "year", "gdp", "gdp_per_capita", "population"]])

    dataset.add(table)
