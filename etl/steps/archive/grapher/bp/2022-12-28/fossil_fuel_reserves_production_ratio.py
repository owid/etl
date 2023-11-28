from owid import catalog

from etl.paths import DATA_DIR

DATASET_SHORT_NAME = "fossil_fuel_reserves_production_ratio"
DATASET_PATH = DATA_DIR / "garden" / "bp" / "2022-12-28" / DATASET_SHORT_NAME


def run(dest_dir: str) -> None:
    # Load dataset from garden.
    garden_dataset = catalog.Dataset(DATASET_PATH)
    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    # Prepare table for grapher.
    table = garden_dataset[DATASET_SHORT_NAME].reset_index()
    # Add table and save dataset.
    dataset.add(table)
    dataset.save()
