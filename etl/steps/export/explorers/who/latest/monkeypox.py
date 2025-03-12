"""Load a grapher dataset and create an explorer dataset with its tsv file.

This step contains tooling that should moved to a more general module so that it can be easily used by other explorer steps!
"""

from etl.collections.explorers import create_explorer
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load grapher config from YAML
    config = paths.load_explorer_config()

    # Create explorer
    ds_explorer = create_explorer(
        dest_dir=dest_dir,
        config=config,
        paths=paths,
        tolerate_extra_indicators=True,
    )

    ds_explorer.save()
