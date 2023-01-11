from shared import run_pipeline

from etl.helpers import PathFinder

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    run_pipeline(dest_dir, paths)
