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
    ResponseSet,
    TableResult,
)
from owid.catalog.api.search import SiteSearchAPI
from owid.catalog.api.tables import TablesAPI


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

        # Tables: Catalog datasets
        results = client.tables.search(table="population", namespace="un")
        table = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")

        # Indicators: Semantic search for data series
        results = client.indicators.search("renewable energy")
        variable = client.indicators.fetch("garden/un/2024-07-12/un_wpp/population#population")

        ```
    """

    charts: ChartsAPI
    indicators: IndicatorsAPI
    tables: TablesAPI
    datasets: TablesAPI  # Backwards compatibility alias
    timeout: int
    _site_search: SiteSearchAPI

    def __init__(self, timeout: int = 30) -> None:
        """Initialize the client with all API interfaces.

        Args:
            timeout: HTTP request timeout in seconds. Default 30.
        """
        self.timeout = timeout
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
    "ResponseSet",
    # Exceptions for error handling
    "ChartNotFoundError",
    "LicenseError",
]
