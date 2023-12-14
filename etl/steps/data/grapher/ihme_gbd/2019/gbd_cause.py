from etl.helpers import PathFinder

from .shared import run_wrapper

paths = PathFinder(__file__)

# only include tables containing INCLUDE string, this is useful for debugging
# but should be None before merging to master!!
# TODO: set this to None before merging to master
INCLUDE = "diarrheal_diseases__both_sexes__age_standardized"
# INCLUDE = None


def run(dest_dir: str) -> None:
    ds_garden = paths.load_dataset("gbd_cause")
    run_wrapper(dest_dir, garden_dataset=ds_garden, include=INCLUDE)
