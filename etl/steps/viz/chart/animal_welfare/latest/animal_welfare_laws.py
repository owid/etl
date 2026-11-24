"""Create the multidim of animal welfare laws."""

from shared import METRICS_LAWS, create_config

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Each law already names its animal, so drop the animal dimension.
    c = paths.create_chart(config=create_config(paths, metrics=METRICS_LAWS, animal_dimension=False))
    c.save()
