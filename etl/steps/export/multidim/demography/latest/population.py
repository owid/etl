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

    # Load grapher dataset.
    ds = paths.load_dataset("population")
    tb = ds.read("population", load_data=False)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="population",
        tb=tb,
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
