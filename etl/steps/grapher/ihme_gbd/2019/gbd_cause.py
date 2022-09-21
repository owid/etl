from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names

from .gbd_tools import run_wrapper

N = Names(__file__)

OLD_DATASET_NAME = (
    "IHME - Global Burden of Disease - Deaths and DALYs - Institute for Health Metrics and Evaluation  (2022-04)"
)


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(N.garden_dataset)
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))
    dataset.save()

    run_wrapper(garden_dataset=garden_dataset, dataset=dataset, dims=["sex", "age", "cause"])
