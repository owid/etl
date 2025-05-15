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

    # Create explorer
    explorer = paths.create_collection(
        config=config,
        explorer=True,
    )

    # explorer.save(tolerate_extra_indicators=True)
    explorer.save()
