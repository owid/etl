from owid import catalog

from etl.helpers import PathFinder

# Naming conventions.
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create new empty grapher dataset, using metadata from the garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Load table from garden dataset.
    table = N.garden_dataset["uk_historical_electricity"].reset_index()
    # Add table to new grapher dataset.
    dataset.add(table)
    # Save new dataset.
    dataset.save()
