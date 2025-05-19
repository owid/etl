"""Load a meadow dataset and create a garden dataset."""

from etl.collection import create_collection, expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #

    # Load grapher dataset.
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    ds = paths.load_dataset("corruption_barometer")
    tb = ds.read("corruption_barometer")

    # Create views.
    config_new = expand_config(
        tb,
        dimensions={
            "question": [
                "How many of the following people do you think are involved in corruption, or haven’t you heard enough about them to say? "
            ],
            "institution": [
                "Business executives",
                "Government officials",
                "Judges and magistrates",
                "Local government councilors",
                "MPs or senators",
                "Police",
                "Prime Minister / President",
                "Religious leaders",
                "Tax officials",
            ],
            "answer": [
                "All of them",
                "Don't know / haven't heard",
                "Most of them",
                "Most or all of them",
                "None of them",
                "Some of them",
                "Some or none of them",
            ],
        },
    )

    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]

    c = paths.create_collection(
        config=config,
        tb=tb,
    )

    #
    # Create collection object
    #

    #
    # Save garden dataset.
    #
    c.save()
