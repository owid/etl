"""Load a grapher dataset and create an explorer dataset with its tsv file.

This step contains tooling that should moved to a more general module so that it can be easily used by other explorer steps!
"""

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
    ds_explorer = paths.create_explorer(
        config=config,
    )

    ds_explorer.save(tolerate_extra_indicators=True)
