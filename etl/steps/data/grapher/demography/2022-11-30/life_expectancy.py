from owid import catalog

from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    # get dataset
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    # get table
    table = N.garden_dataset["life_expectancy"]

    # add table
    dataset.add(table)

    # save table
    dataset.save()
