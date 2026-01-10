#
#  owid.catalog.api
#
#  Unified client for all OWID data APIs.
#

from __future__ import annotations

from owid.catalog.api.charts import ChartNotFoundError, ChartResult, ChartsAPI, LicenseError
from owid.catalog.api.client import Client
from owid.catalog.api.indicators import IndicatorResult, IndicatorsAPI
from owid.catalog.api.models import ResponseSet
from owid.catalog.api.quick import fetch, search
from owid.catalog.api.tables import TableResult, TablesAPI

__all__ = [
    # Main client
    "Client",
    # API classes
    "TablesAPI",
    "ChartsAPI",
    "IndicatorsAPI",
    # Result types for type hints
    "ResponseSet",
    "ChartResult",
    "IndicatorResult",
    "TableResult",
    # Exceptions for error handling
    "ChartNotFoundError",
    "LicenseError",
    # Quick API
    "search",
    "fetch",
]
