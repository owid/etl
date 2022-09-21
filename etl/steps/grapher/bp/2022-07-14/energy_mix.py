"""Grapher step for BP's energy mix 2022 dataset.

"""

from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

DATASET_PATH = DATA_DIR / "garden" / "bp" / "2022-07-14" / "energy_mix"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_PATH)
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))

    # There is only one table in the dataset, with the same name as the dataset.
    table = garden_dataset[garden_dataset.table_names[0]].reset_index().drop(columns=["country_code"])
    dataset.add(gh.adapt_table_for_grapher(table))
    dataset.save()
