from owid import catalog

from etl.paths import DATA_DIR

DATASET_GARDEN = DATA_DIR / "garden/owid/latest/internet"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_GARDEN)
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)

    # Add population table to dataset
    dataset.add(garden_dataset["users"])

    # Save dataset
    dataset.save()
