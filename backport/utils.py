from owid.catalog.utils import underscore


def create_short_name(dataset_id: int, dataset_name: str) -> str:
    """Create sensible short name for dataset."""
    # prepend dataset id to short name
    return f"dataset_{dataset_id}_{underscore(dataset_name)}"


def extract_id_from_short_name(short_name: str) -> int:
    return int(short_name.split("_")[1])
