from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    c = paths.create_collection(
        config=paths.load_collection_config(),
        short_name="migrant_populations",
    )
    c.save()
