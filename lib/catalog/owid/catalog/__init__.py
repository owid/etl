__version__ = "1.0.0rc2"

from owid.catalog import api, processing, utils
from owid.catalog.api import Client, fetch, search
from owid.catalog.catalogs import CHANNEL
from owid.catalog.core.datasets import Dataset
from owid.catalog.core.indicators import Indicator, Variable
from owid.catalog.core.meta import (
    DatasetMeta,
    FaqLink,
    License,
    Origin,
    Source,
    TableMeta,
    VariableMeta,
    VariablePresentationMeta,
)
from owid.catalog.core.processing_log import LogEntry, ProcessingLog
from owid.catalog.core.tables import Table

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
    "Indicator",
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
