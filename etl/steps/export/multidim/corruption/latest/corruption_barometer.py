"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "chartTypes": [],
    "tab": "map",
}


def run() -> None:
    #
    # Load inputs.
    #

    # Load grapher dataset.
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    ds = paths.load_dataset("corruption_barometer")
    tb = ds.read("corruption_barometer")
    c = paths.create_collection(
        config=config,
        tb=tb,
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
                "Most or all of them",
                "Some or none of them",
                "Don't know / haven't heard",
            ],
        },
        common_view_config=MULTIDIM_CONFIG,
    )

    #
    # Create collection object
    #

    #
    # Save garden dataset.
    #
    c.save()
