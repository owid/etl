__version__ = "0.4.0"

from . import client, processing, utils
from .catalogs import CHANNEL, LocalCatalog, RemoteCatalog, find, find_by_indicator, find_latest, find_one
from .client import Client
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
    "client",
    # Legacy catalog access (still supported)
    "LocalCatalog",
    "RemoteCatalog",
    "find",
    "find_by_indicator",
    "find_latest",
    "find_one",
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
