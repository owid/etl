"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_collection_config()

    # Create collection
    c = paths.create_collection(
        config=config,
        short_name="water-and-sanitation",
        explorer=True,
    )

    # Edit display
    for view in c.views:
        assert view.indicators.y is not None
        for y in view.indicators.y:
            y.update_display(
                {
                    "colorScaleNumericMinValue": 0,
                }
            )

    c.save()
