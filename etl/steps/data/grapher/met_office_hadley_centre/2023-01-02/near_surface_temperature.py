"""Load garden dataset of near surface temperature by Met Office Hadley Centre, and create a grapher dataset.

"""

from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load table from garden dataset.
    table = N.garden_dataset["near_surface_temperature"].reset_index()

    # For compatibility with grapher, change the name of "region" column to "country".
    table = table.rename(columns={"region": "country"})

    # Create new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    dataset.metadata.short_name = N.short_name
    # Add table to dataset and save dataset.
    dataset.add(table)
    dataset.save()
