from structlog import get_logger

from etl.helpers import PathFinder

from .shared import run_wrapper

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    dataset = paths.short_name
    metadata_path = paths.metadata_path
    namespace = paths.namespace
    version = paths.version
    log.info(f"{dataset}.start")
    run_wrapper(dataset, metadata_path.as_posix(), namespace, version, dest_dir)
    log.info(f"{dataset}.end")
