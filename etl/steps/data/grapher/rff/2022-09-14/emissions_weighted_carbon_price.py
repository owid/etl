from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create new grapher dataset with the metadata from the garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.metadata.short_name = N.short_name
    # Load table from garden dataset.
    table = N.garden_dataset["emissions_weighted_carbon_price"].reset_index()
    # Add table to new grapher dataset.
    dataset.add(table)
    # Save dataset.
    dataset.save()
