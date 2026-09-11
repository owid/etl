"""Load grapher dataset and create the fish-stocks explorer tsv file."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_config()

    c = paths.create_explorer(
        config=config,
        short_name="fish-stocks",
    )

    c.save(tolerate_extra_indicators=True)
