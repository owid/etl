"""Temporary multidimensional chart, to compare production-based and consumption-based numbers of animals killed."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(
        config=paths.load_config(),
        short_name="animals-killed-production-vs-consumption",
    )
    c.save()
