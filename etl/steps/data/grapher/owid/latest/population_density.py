from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    # NOTE: this generates shortName `population_density__owid_latest`, perhaps we should keep it as `population_density`
    # and create unique constraint on (shortName, version, namespace) instead of just (shortName, namespace)
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(N.garden_dataset.metadata))
    dataset.save()

    table = gh.adapt_table_for_grapher(N.garden_dataset["population_density"].reset_index())

    dataset.add(table)
