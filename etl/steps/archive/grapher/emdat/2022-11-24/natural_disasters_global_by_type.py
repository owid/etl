from owid import catalog
from shared import DISASTER_TYPE_RENAMING, GARDEN_DATASET_PATH, GARDEN_VERSION_YEAR

GRAPHER_DATASET_TITLE = f"Global natural disasters by type (EM-DAT, {GARDEN_VERSION_YEAR})"
GRAPHER_DATASET_SHORT_NAME = "natural_disasters_global_by_type"


def run(dest_dir: str) -> None:
    # Load garden dataset.
    garden_dataset = catalog.Dataset(GARDEN_DATASET_PATH)
    # Load table on yearly data.
    table = garden_dataset["natural_disasters_yearly"].reset_index()

    # Select data for the World and remove unnecessary columns.
    table_global = (
        table[table["country"] == "World"].drop(columns=["country", "population", "gdp"]).reset_index(drop=True)
    )
    # Treat column for disaster type as the new entity (so they can be selected in grapher as if they were countries).
    table_global = table_global.rename(columns={"type": "country"}).replace(DISASTER_TYPE_RENAMING)

    # Set an appropriate index.
    table_global = table_global.set_index(["country", "year"]).sort_index()

    # Create new grapher dataset, update metadata, add table, and save dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.metadata.title = GRAPHER_DATASET_TITLE
    dataset.metadata.short_name = GRAPHER_DATASET_SHORT_NAME
    table_global.metadata.title = GRAPHER_DATASET_TITLE
    table_global.metadata.short_name = GRAPHER_DATASET_SHORT_NAME
    dataset.add(table_global)
    dataset.save()
