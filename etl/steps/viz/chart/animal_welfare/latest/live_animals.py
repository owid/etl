"""Create the multidim of live animals, including hens in cages and cage-free systems."""

from shared import METRICS_LIVE_ANIMALS, create_config

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(config=create_config(paths, metrics=METRICS_LIVE_ANIMALS))
    c.save()
