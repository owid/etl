# Stub file for backwards compatibility - re-exports from core/meta.py
# New code should import from owid.catalog.core.meta
from owid.catalog.core.meta import (
    DatasetMeta,
    License,
    MetaBase,
    Origin,
    Source,
    T,
    TableDimension,
    TableMeta,
    VariableMeta,
    to_html,
)

# pruned_json is re-exported from utils for backwards compatibility
from owid.catalog.core.utils import pruned_json

__all__ = [
    "T",
    "MetaBase",
    "License",
    "Source",
    "Origin",
    "VariableMeta",
    "DatasetMeta",
    "TableDimension",
    "TableMeta",
    "to_html",
    "pruned_json",
]
