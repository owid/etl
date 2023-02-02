from owid import catalog

from etl.helpers import PathFinder

from .gbd_tools import run_wrapper

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    garden_dataset = N.garden_dataset
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    dataset.save()

    run_wrapper(garden_dataset=garden_dataset, dataset=dataset, dims=["sex", "age", "cause"])
