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

    # Charts API - fetch data from published charts
    df = client.charts.get_data("life-expectancy")
    chart = client.charts.fetch("life-expectancy")  # Full chart object

    # Search API - find charts and pages
    charts = client.search.charts("gdp per capita")
    pages = client.search.pages("climate change")

    # Indicators API - semantic search for indicators
    indicators = client.indicators.search("renewable energy")
    table = indicators[0].load()

    # Datasets API - query and load from catalog
    results = client.datasets.find(table="population", namespace="un")
    table = client.datasets.find_one(table="gdp", namespace="worldbank")
    table = client.datasets["garden/un/2024/population/population"]
    ```
"""

from __future__ import annotations

from .charts import ChartNotFoundError, ChartsAPI, LicenseError
from .datasets import DatasetsAPI
from .indicators import IndicatorsAPI
from .models import (
    ChartResult,
    ChartSearchResult,
    DatasetResult,
    IndicatorResult,
    PageSearchResult,
    ResultSet,
)
from .search import SearchAPI


class Client:
    """Unified client for all OWID data APIs.

    Provides access to four main APIs:
    - charts: Fetch data and metadata from published charts
    - search: Find charts and pages via Algolia search
    - indicators: Semantic search for data indicators
    - datasets: Query and load from the data catalog

    Attributes:
        charts: ChartsAPI instance for chart operations.
        search: SearchAPI instance for content search.
        indicators: IndicatorsAPI instance for indicator search.
        datasets: DatasetsAPI instance for catalog operations.

    Example:
        ```python
        from owid.catalog.client import Client

        # Create client (recommended: reuse for multiple operations)
        client = Client()

        # Access different APIs
        df = client.charts.get("life-expectancy")
        results = client.search.charts("gdp")
        indicators = client.indicators.search("solar energy")
        table = client.datasets.find_one(table="population")
        ```
    """

    def __init__(self) -> None:
        """Initialize the client with all API interfaces."""
        self.charts = ChartsAPI(self)
        self.search = SearchAPI(self)
        self.indicators = IndicatorsAPI(self)
        self.datasets = DatasetsAPI(self)

    def __repr__(self) -> str:
        return "Client(charts=..., search=..., indicators=..., datasets=...)"


__all__ = [
    # Main client
    "Client",
    # API classes
    "ChartsAPI",
    "SearchAPI",
    "IndicatorsAPI",
    "DatasetsAPI",
    # Result types
    "ChartResult",
    "ChartSearchResult",
    "PageSearchResult",
    "IndicatorResult",
    "DatasetResult",
    "ResultSet",
    # Exceptions
    "ChartNotFoundError",
    "LicenseError",
]
