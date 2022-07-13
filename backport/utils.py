from owid.catalog.utils import underscore

from etl.grapher_model import GrapherDatasetModel


def create_short_name(ds: GrapherDatasetModel) -> str:
    """Create sensible short name for dataset."""
    # prepend dataset id to short name
    return f"dataset_{ds.id}_{underscore(ds.name)}"


def extract_id_from_short_name(short_name: str) -> int:
    return int(short_name.split("_")[1])
