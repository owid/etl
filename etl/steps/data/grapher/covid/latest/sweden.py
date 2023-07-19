from etl.helpers import PathFinder

from .shared import run as shared_run

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    return shared_run(dest_dir, paths)
