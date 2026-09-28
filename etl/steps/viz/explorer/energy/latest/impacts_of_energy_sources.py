"""Load grapher dataset and create the Impacts of Energy Production explorer tsv file."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_config()

    #
    # Save outputs.
    #
    c = paths.create_explorer(
        config=config,
        short_name="impacts-of-energy-sources",
    )

    c.save(tolerate_extra_indicators=True)
