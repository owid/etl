"""Chart of people affected by disasters vs. GDP per capita."""

from shared import get_last_fully_reported_year

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_config()
    tb = paths.load_dataset("natural_disasters").read("natural_disasters_yearly", load_data=False)

    #
    # Process data.
    #
    view_config = config["views"][0]["config"]
    # End the timeline at the last fully reported year, like the natural disasters multidims.
    view_config["timelineMaxTime"] = get_last_fully_reported_year(tb)

    #
    # Save outputs.
    #
    c = paths.create_chart(config=config)
    c.save()
