#
#  owid.catalog.client
#
#  Unified client for all OWID data APIs.
#
"""
Unified client for accessing Our World in Data APIs.

Example:
    ```python
    from owid.catalog.client import Client

    client = Client()

    # Charts API - fetch and search for published charts
    df = client.charts.get_data("life-expectancy")
    chart = client.charts.fetch("life-expectancy")  # Fetch specific chart
    df = chart.data  # Lazy-load data via property
    results = client.charts.search("gdp per capita")  # Search for charts

    # Indicators API - semantic search for indicators
    indicators = client.indicators.search("renewable energy")
    indicator = client.indicators.fetch(12345)  # Fetch by ID
    variable = indicator.data  # Returns Variable (Series), not Table
    table = indicator.table  # Access full table if needed

    # Tables API - query and load from catalog
    results = client.tables.search(table="population", namespace="un")
    table_result = client.tables.fetch("garden/un/2024/pop/pop")  # Fetch metadata
    table = table_result.data  # Lazy-load table data
    table = client.tables["garden/un/2024/population/population"]  # Direct access

    # Backwards compatibility: client.datasets still works (deprecated)
    results = client.datasets.search(table="population")  # Works, but use .tables instead

    # Advanced: Search pages/articles
    pages = client._site_search.pages("climate change")
    ```
"""

from __future__ import annotations

from .charts import ChartNotFoundError, ChartsAPI, LicenseError
from .indicators import IndicatorsAPI
from .models import (
    ChartResult,
    IndicatorResult,
    PageSearchResult,
    ResultSet,
    TableResult,
)
from .search import SiteSearchAPI
from .tables import TablesAPI

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

    Example: test
        ```python
        from owid.catalog.client import Client

        # Create client (recommended: reuse for multiple operations)
        client = Client()

        # Charts API
        chart = client.charts.fetch("gdp")  # Fetch specific chart
        df = client.charts.get_data("life-expectancy")
        results = client.charts.search("population")  # Search for charts

        # Indicators API
        indicators = client.indicators.search("solar energy")
        indicator = client.indicators.fetch(12345)  # Fetch by ID

        # Tables API
        results = client.tables.search(table="population", namespace="un")
        table_result = client.tables.fetch("garden/un/2024/pop/pop")  # Fetch metadata
        table = results[0].data  # Lazy-load data
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
