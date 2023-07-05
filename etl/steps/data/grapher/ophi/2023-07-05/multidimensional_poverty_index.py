"""
This code generates the grapher step for the MPI dataset
"""

from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    table = N.garden_dataset["multidimensional_poverty_index"]

    # if your data is in long format, you can use `grapher_helpers.long_to_wide_tables`
    # to get into wide format
    dataset.add(table)

    dataset.save()
