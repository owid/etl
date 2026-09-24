"""Multidimensional chart of the animals used for food, by animal and by how they are counted."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_chart(
        config=paths.load_config(),
        short_name="animals-used-for-food",
    )
    c.save()
