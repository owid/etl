from typing import Optional

from owid.catalog.utils import validate_underscore


def create_short_name(short_name: Optional[str], dataset_id: int) -> str:
    """Create sensible short name for dataset."""
    validate_underscore(short_name, "short-name")
    # prepend dataset id to short name
    return f"dataset_{dataset_id}_{short_name}"


def extract_id_from_short_name(short_name: str) -> int:
    return int(short_name.split("_")[1])
