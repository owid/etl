"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="ilostat_national_vs_modeled",
    )

    #
    # Save garden dataset.
    #
    c.save()
