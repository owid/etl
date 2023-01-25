from gbd_tools import run_wrapper
from structlog import get_logger

from etl.helpers import PathFinder

# naming conventions
N = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:

    dims = ["sex", "age", "cause", "rei"]
    # Get dataset level variables
    dataset = N.short_name
    log.info(f"{dataset}.start")
    country_mapping_path = N.directory / "gbd.countries.json"
    excluded_countries_path = N.directory / "gbd.excluded_countries.json"
    metadata_path = N.directory / f"{dataset}.meta.yml"
    # Run the function to produce garden dataset
    run_wrapper(dataset, country_mapping_path, excluded_countries_path, dest_dir, metadata_path, dims)
    log.info(f"{dataset}.end")
