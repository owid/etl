__version__ = "0.4.0"

from . import api, processing, utils
from .api import Client
from .catalogs import CHANNEL, ETLCatalog, LocalCatalog, find
from .datasets import Dataset
from .meta import (
    DatasetMeta,
    FaqLink,
    License,
    Origin,
    Source,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
)
from .processing_log import LogEntry, ProcessingLog
from .tables import Table
from .variables import Variable

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
