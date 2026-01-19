# Stub file for backwards compatibility - re-exports from core/yaml_metadata.py
# New code should import from owid.catalog.core.yaml_metadata
from owid.catalog.core.yaml_metadata import (
    merge_with_shared_meta,
    update_metadata_from_yaml,
)

__all__ = [
    "merge_with_shared_meta",
    "update_metadata_from_yaml",
]
