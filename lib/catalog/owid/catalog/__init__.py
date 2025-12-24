__version__ = "1.0.0rc1"

from owid.catalog import api, processing, utils
from owid.catalog.api import Client
from owid.catalog.catalogs import CHANNEL, ETLCatalog, LocalCatalog, find
from owid.catalog.datasets import Dataset
from owid.catalog.meta import (
    DatasetMeta,
    FaqLink,
    License,
    Origin,
    Source,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
)
from owid.catalog.processing_log import LogEntry, ProcessingLog
from owid.catalog.tables import Table
from owid.catalog.variables import Variable

__all__ = [
    # New unified client API
    "Client",
    "api",
    # Legacy catalog access (still supported)
    "LocalCatalog",
    "ETLCatalog",
    "find",
    # Core data structures
    "Dataset",
    "Table",
    "Variable",
    # Metadata classes
    "DatasetMeta",
    "TableMeta",
    "VariableMeta",
    "VariablePresentationMeta",
    "LogEntry",
    "ProcessingLog",
    "FaqLink",
    "Source",
    "Origin",
    "License",
    # Utilities
    "utils",
    "CHANNEL",
    "processing",
]
