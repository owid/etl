from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    # Load table from garden.
    table = N.garden_dataset["global_carbon_budget_additional"].reset_index()
    # Add table to dataset.
    dataset.add(table)
    # Save dataset.
    dataset.save()
