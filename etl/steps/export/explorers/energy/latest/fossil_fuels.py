"""Load the fossil_fuels grapher dataset and create the fossil fuels explorer.

"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_collection_config()

    #
    # Process data.
    #
    c = paths.create_collection(
        config=config,
        short_name="fossil-fuels",
        explorer=True,
    )

    #
    # Save outputs.
    #
    c.save()
