from owid import catalog

from etl.helpers import create_dataset
from etl.paths import DATA_DIR

# Path to input garden dataset.
GARDEN_DATASET_PATH = DATA_DIR / "garden/andrew/2019-12-03/co2_mitigation_curves"
GARDEN_TABLE_NAME = "co2_mitigation_curves_1p5celsius"


def run(dest_dir: str) -> None:
    # Load garden dataset.
    garden_dataset = catalog.Dataset(GARDEN_DATASET_PATH)
    # Load necessary table from garden dataset.
    table = garden_dataset[GARDEN_TABLE_NAME].reset_index()
    # Convert units conveniently.
    old_title = table["emissions"].metadata.title
    old_description = table["emissions"].metadata.description
    table["emissions"] *= 1e9
    table["emissions"].metadata.unit = "tonnes"
    table["emissions"].metadata.short_unit = "t"
    table["emissions"].metadata.title = old_title
    table["emissions"].metadata.description = old_description
    # Load table from dataset and change the "origin" column to act as if it was the country name.
    # This is a workaround to be able to visualize all curves of origin together in a line chart.
    table = table.rename(columns={"origin": "country"})
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[table], default_metadata=garden_dataset.metadata)
    ds_grapher.metadata.title = table.metadata.title
    ds_grapher.save()
