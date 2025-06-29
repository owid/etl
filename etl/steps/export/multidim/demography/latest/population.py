"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Create collection object
    #
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="population",
        # indicator_names=[],
        # dimensions={},
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimension["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save garden dataset.
    #
    c.save()
