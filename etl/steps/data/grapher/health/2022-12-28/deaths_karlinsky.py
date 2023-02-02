from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


TABLE_NAME = "deaths"


def run(dest_dir: str) -> None:
    # get dataset from garden
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # get table from garden
    table = N.garden_dataset[TABLE_NAME]

    # add table
    dataset.add(table)

    dataset.save()
