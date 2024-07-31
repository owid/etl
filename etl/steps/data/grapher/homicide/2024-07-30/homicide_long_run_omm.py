from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    dataset.metadata.version = "2023-01-03"

    table = N.garden_dataset["homicide_long_run_omm"]

    dataset.add(table)

    dataset.save()
