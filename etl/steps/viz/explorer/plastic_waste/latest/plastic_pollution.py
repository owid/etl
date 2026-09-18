"""Build the plastic-pollution explorer."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_config()

    c = paths.create_explorer(
        config=config,
        short_name="plastic-pollution",
    )

    c.save(tolerate_extra_indicators=True)
