from etl.helpers import PathFinder

from .shared import run_wrapper

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_garden = paths.load_dataset("gbd_prevalence")
    run_wrapper(dest_dir, garden_dataset=ds_garden)
