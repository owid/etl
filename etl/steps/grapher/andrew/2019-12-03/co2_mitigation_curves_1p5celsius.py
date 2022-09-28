from owid import catalog

from etl.paths import DATA_DIR

# Path to input garden dataset.
GARDEN_DATASET_PATH = DATA_DIR / "garden/andrew/2019-12-03/co2_mitigation_curves"
GARDEN_TABLE_NAME = "co2_mitigation_curves_1p5celsius"


def run(dest_dir: str) -> None:
    # Load garden dataset.
    garden_dataset = catalog.Dataset(GARDEN_DATASET_PATH)
    # Load necessary table from garden dataset.
    table = garden_dataset[GARDEN_TABLE_NAME].reset_index()
    # Use dataset metadata to match the metadata of the specific table we need.
    garden_dataset.metadata.short_name = GARDEN_TABLE_NAME
    garden_dataset.metadata.title = table.metadata.title
    garden_dataset.metadata.description = table.metadata.description

    # Create a new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir,garden_dataset.metadata)
    # Load table from dataset and change the "origin" column to act as if it was the country name.
    # This is a workaround to be able to visualize all curves of origin together in a line chart.
    table = table.rename(columns={"origin": "country"})
    # Add table to grapher dataset and save it.
    dataset.add(table)
    dataset.save()
