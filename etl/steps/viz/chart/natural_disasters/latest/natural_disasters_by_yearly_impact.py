"""Chart of the global number of reported disasters by size."""

from shared import LAST_FULLY_REPORTED_YEAR

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_config()

    #
    # Process data.
    #
    # End the timeline at the last fully reported year, like the natural disasters multidims.
    config["views"][0]["config"]["timelineMaxTime"] = LAST_FULLY_REPORTED_YEAR

    #
    # Save outputs.
    #
    c = paths.create_chart(config=config)
    c.save()
