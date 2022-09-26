from owid import catalog

from etl.helpers import Names

N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/data/grapher/who/2022-07-17/who_vaccination.py")


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)
    table = N.garden_dataset["who_vaccination"]
    dataset.add(table)
    dataset.save()
