from structlog import get_logger

from etl.helpers import Names

from .shared import run_wrapper

log = get_logger()

# naming conventions
N = Names(__file__)
N = Names('/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/data/meadow/ihme_gbd/2019/gbd_cause.py')

def run(dest_dir: str) -> None:
    dataset = N.short_name
    metadata_path = N.metadata_path
    namespace = N.namespace
    version = N.version
    log.info(f"{dataset}.start")
    run_wrapper(dataset, metadata_path, namespace, version, dest_dir)
    log.info(f"{dataset}.end")
