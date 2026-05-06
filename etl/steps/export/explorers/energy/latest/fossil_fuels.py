"""Load the fossil_fuels grapher dataset and create the natural-resources explorer tsv file.

The explorer slug is kept as "natural-resources" for URL backward compatibility — the live
explorer lives at /explorers/natural-resources, even though its title is "Fossil Fuels" and the
underlying dataset is fossil_fuels.
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
        short_name="natural-resources",
        explorer=True,
    )

    #
    # Save outputs.
    #
    c.save()
