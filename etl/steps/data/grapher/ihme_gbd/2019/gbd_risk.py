from owid import catalog
from shared import run_wrapper

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    garden_dataset = paths.garden_dataset
    dataset = catalog.Dataset.create_empty(dest_dir, garden_dataset.metadata)
    run_wrapper(garden_dataset=garden_dataset, dataset=dataset)
