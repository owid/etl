from owid import catalog

from etl.helpers import Names

N = Names(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    table = N.garden_dataset["renewable_electricity_capacity"]
    dataset.add(table)
    dataset.save()
