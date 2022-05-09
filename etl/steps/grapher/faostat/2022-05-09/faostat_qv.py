from .shared import catalog, get_grapher_dataset_from_file_name
from .shared import get_grapher_tables  # noqa:F401


def get_grapher_dataset() -> catalog.Dataset:
    return get_grapher_dataset_from_file_name(__file__)
