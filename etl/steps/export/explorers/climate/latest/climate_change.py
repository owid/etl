"""Load grapher datasets and create an explorer tsv file."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_explorer_config()

    # Create explorer
    ds_explorer = paths.create_collection(
        config=config,
        short_name="climate-change",
        explorer=True,
    )

    #
    # Save outputs.
    #
    ds_explorer.save(tolerate_extra_indicators=True)
