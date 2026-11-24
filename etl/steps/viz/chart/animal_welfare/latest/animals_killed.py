"""Create the multidim of animals killed for food, counted in the country or for the country's consumption."""

from shared import METRICS_ANIMALS_KILLED, create_config

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(config=create_config(paths, metrics=METRICS_ANIMALS_KILLED))
    c.save()
