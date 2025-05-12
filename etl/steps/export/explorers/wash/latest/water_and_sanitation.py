"""Load a grapher dataset and create an explorer dataset with its tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_explorer_config()

    # Set equal size color bins
    for view in config["views"]:
        for y in view["indicators"]["y"]:
            y["display"]["colorScaleEqualSizeBins"] = True
            y["display"]["colorScaleNumericMinValue"] = 0

    # Create explorer
    explorer = paths.create_collection_legacy(
        config=config,
        short_name="water-and-sanitation",
        explorer=True,
    )

    explorer.save()
