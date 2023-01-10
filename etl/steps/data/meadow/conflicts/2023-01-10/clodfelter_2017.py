from shared import run_conflicts

from etl.helpers import PathFinder

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    run_conflicts(dest_dir, paths)
