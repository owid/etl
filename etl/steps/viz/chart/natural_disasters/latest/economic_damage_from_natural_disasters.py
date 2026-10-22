"""Chart of economic damage by natural disaster type."""

from shared import get_date_published, get_last_fully_reported_year

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_config()
    tb = paths.load_dataset("natural_disasters_global_by_type").read(
        "natural_disasters_global_by_type", load_data=False
    )

    #
    # Process data.
    #
    view_config = config["views"][0]["config"]
    # End the timeline at the last fully reported year, like the natural disasters multidims.
    view_config["timelineMaxTime"] = get_last_fully_reported_year(tb)
    # Write the month of the EM-DAT release into the footnote.
    view_config["note"] = view_config["note"].format(date_published=f"{get_date_published(tb):%B %Y}")

    #
    # Save outputs.
    #
    c = paths.create_chart(config=config)
    c.save()
