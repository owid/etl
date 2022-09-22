from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names
from etl.paths import DATA_DIR

from .gbd_tools import run_wrapper

N = Names(__file__)


def run(dest_dir: str) -> None:
    garden_dataset = N.garden_dataset
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))
    dataset.save()

    run_wrapper(garden_dataset=garden_dataset, dataset=dataset, dims=["sex", "age", "cause", "rei"])
