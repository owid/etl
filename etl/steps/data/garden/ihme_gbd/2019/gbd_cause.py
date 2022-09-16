from gbd_tools import run_wrapper
from structlog import get_logger

from etl.helpers import Names

# naming conventions
N = Names(__file__)
N = Names("/Users/fionaspooner/Documents/OWID/repos/etl/etl/steps/data/garden/ihme_gbd/2019/gbd_cause.py")
log = get_logger()


def run(dest_dir: str) -> None:

    # Get dataset level variables
    dataset = N.short_name
    log.info(f"{dataset}.start")
    country_mapping_path = N.directory / "gbd.countries.json"
    excluded_countries_path = N.directory / "gbd.excluded_countries.json"
    metadata_path = N.directory / f"{dataset}.meta.yml"
    # Run the function to produce garden dataset
    run_wrapper(dataset, country_mapping_path, excluded_countries_path, dest_dir, metadata_path)
    log.info(f"{dataset}.end")
