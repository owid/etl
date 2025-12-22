#
#  owid.catalog.api
#
#  Unified client for all OWID data APIs.
#

from __future__ import annotations

from owid.catalog.api.charts import ChartsAPI
from owid.catalog.api.indicators import IndicatorsAPI
from owid.catalog.api.models import (
    ChartNotFoundError,
    ChartResult,
    IndicatorResult,
    LicenseError,
    PageSearchResult,
    ResultSet,
    TableResult,
)
from owid.catalog.api.search import SiteSearchAPI
from owid.catalog.api.tables import TablesAPI

# Backwards compatibility aliases
DatasetResult = TableResult
DatasetsAPI = TablesAPI


class Client:
    """Unified client for all OWID data APIs.

    Provides access to our main APIs:

    - ChartsAPI: Fetch and search for published charts
    - IndicatorsAPI: Semantic search for data indicators
    - TablesAPI: Query and load tables from the data catalog

    Attributes:
        charts: ChartsAPI instance for chart operations and search.
        indicators: IndicatorsAPI instance for indicator search.
        tables: TablesAPI instance for catalog operations.
        datasets: Deprecated alias for tables (backwards compatibility).

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Charts: Published visualizations
        results = client.charts.search("climate change")
        chart = client.charts.fetch("life-expectancy")
        df = client.charts.get_data("animals-slaughtered-for-meat")

        # Tables: Catalog datasets
        results = client.tables.search(table="population", namespace="un")
        table = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
        tb = client.tables.get_data("garden/un/2024-07-12/un_wpp/population")

        # Indicators: Semantic search for data series
        results = client.indicators.search("renewable energy")
        variable = client.indicators.fetch("garden/un/2024-07-12/un_wpp/population#population")
        data = client.indicators.get_data("garden/un/2024-07-12/un_wpp/population#population")

        ```
    """

    charts: ChartsAPI
    indicators: IndicatorsAPI
    tables: TablesAPI
    datasets: TablesAPI  # Backwards compatibility alias
    _site_search: SiteSearchAPI

    def __init__(self) -> None:
        """Initialize the client with all API interfaces."""
        self.charts = ChartsAPI(self)
        self.indicators = IndicatorsAPI(self)
        self.tables = TablesAPI(self)
        self.datasets = self.tables  # Backwards compatibility alias
        self._site_search = SiteSearchAPI(self)

    def __repr__(self) -> str:
        return "Client(charts=..., indicators=..., tables=...)"


__all__ = [
    # Main client
    "Client",
    # API classes
    "TablesAPI",
    "ChartsAPI",
    "IndicatorsAPI",
    # Result types for type hints
    "ChartResult",
    "PageSearchResult",
    "IndicatorResult",
    "TableResult",
    "ResultSet",
    # Backwards compatibility aliases (deprecated)
    "DatasetResult",  # Alias for TableResult
    "DatasetsAPI",  # Alias for TablesAPI
    # Exceptions for error handling
    "ChartNotFoundError",
    "LicenseError",
]
