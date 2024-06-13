from owid import catalog

from etl.paths import DATA_DIR

DATASET_GARDEN = DATA_DIR / "garden/research_development/2024-05-20/patents_articles"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_GARDEN)
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)

    # Add population table to dataset
    dataset.add(garden_dataset["patents_articles"])

    # Save dataset
    dataset.save()
