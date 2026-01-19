# Stub file for backwards compatibility - re-exports from core/meta.py
# New code should import from owid.catalog.core.meta
from owid.catalog.core.meta import (
    PROCESSING_LEVELS,
    PROCESSING_LEVELS_ORDER,
    SOURCE_EXISTS_OPTIONS,
    VARIABLE_TYPE,
    DatasetMeta,
    FaqLink,
    GrapherConfig,
    License,
    MetaBase,
    Origin,
    Source,
    T,
    TableDimension,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
    YearDateLatest,
    is_year_or_date,
    to_html,
    update_variable_metadata,
)

# pruned_json is re-exported from utils for backwards compatibility
from owid.catalog.core.utils import pruned_json

__all__ = [
    "SOURCE_EXISTS_OPTIONS",
    "VARIABLE_TYPE",
    "YearDateLatest",
    "T",
    "MetaBase",
    "License",
    "Source",
    "Origin",
    "PROCESSING_LEVELS",
    "PROCESSING_LEVELS_ORDER",
    "FaqLink",
    "GrapherConfig",
    "VariablePresentationMeta",
    "VariableMeta",
    "DatasetMeta",
    "TableDimension",
    "TableMeta",
    "to_html",
    "is_year_or_date",
    "update_variable_metadata",
    "pruned_json",
]
