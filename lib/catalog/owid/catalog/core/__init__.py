#
#  owid.catalog.core
#
#  Core utilities for the OWID catalog.
#
from owid.catalog.core.indicators import (
    Variable,
)
from owid.catalog.core.meta import (
    DatasetMeta,
    License,
    MetaBase,
    Origin,
    Source,
    TableDimension,
    TableMeta,
    VariableMeta,
    to_html,
)
from owid.catalog.core.paths import CatalogPath
from owid.catalog.core.tables import Table
from owid.catalog.core.warnings import (
    NoOriginsWarning,
    catch_warnings,
    ignore_warnings,
    simplefilter,
    warn,
)

__all__ = [
    "CatalogPath",
    # tables
    "Table",
    # properties
    # warnings
    "NoOriginsWarning",
    "ignore_warnings",
    "catch_warnings",
    "simplefilter",
    "warn",
    # meta
    "MetaBase",
    "License",
    "Source",
    "Origin",
    "VariableMeta",
    "DatasetMeta",
    "TableDimension",
    "TableMeta",
    "to_html",
    # indicators
    "Variable",
]
