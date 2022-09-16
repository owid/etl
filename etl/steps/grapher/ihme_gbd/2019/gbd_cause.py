from owid import catalog

from etl import grapher_helpers as gh
from etl.helpers import Names
from etl.paths import DATA_DIR

from .gbd_tools import run_wrapper

N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/grapher/ihme_gbd/2019/gbd_cause.py")


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATA_DIR / f"garden/{N.namespace}/{N.version}/{N.short_name}")
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))
    # short_name should include dataset name and version
    dataset.metadata.short_name = f"{garden_dataset.metadata.short_name}__{N.version.replace('-', '_')}"
    dataset.save()

    run_wrapper(
        garden_dataset=garden_dataset,
        dataset=dataset,
        old_dataset_name="IHME - Global Burden of Disease - Deaths and DALYs - Institute for Health Metrics and Evaluation  (2022-04)",
    )
