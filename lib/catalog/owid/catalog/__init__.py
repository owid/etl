__version__ = "1.0.0rc2"

from owid.catalog import api, processing, utils
from owid.catalog.api import Client, fetch, search
from owid.catalog.catalogs import CHANNEL
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
    "search",
    "fetch",
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
