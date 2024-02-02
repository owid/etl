"""Grapher step for the Electricity Mix (BP & Ember, 2022) dataset.
"""

from copy import deepcopy

from owid import catalog

from etl.paths import DATA_DIR

# Path to garden dataset to be loaded.
DATASET_PATH = DATA_DIR / "garden" / "energy" / "2022-12-28" / "electricity_mix"
TABLE_NAME = "electricity_mix"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_PATH)
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)

    # There is only one table in the dataset, with the same name as the dataset.
    table = garden_dataset[TABLE_NAME].reset_index().drop(columns=["population"])

    # Add zero-filled variables (where missing points are filled with zeros) to avoid stacked area charts
    # showing incomplete data.
    generation_columns = [c for c in table.columns if "generation__twh" in c]
    for column in generation_columns:
        new_column = f"{column}_zero_filled"
        table[new_column] = table[column].fillna(0)
        table[new_column].metadata = deepcopy(table[column].metadata)
        table[new_column].metadata.title += " (zero filled)"

    dataset.add(table)
    dataset.save()
