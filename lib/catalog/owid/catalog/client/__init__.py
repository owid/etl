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
    results = client.charts.search("gdp per capita")  # Search for charts

    # Indicators API - semantic search for indicators
    indicators = client.indicators.search("renewable energy")
    indicator = client.indicators.fetch(12345)  # Fetch by ID
    table = indicators[0].load()

    # Datasets API - query and load from catalog
    results = client.datasets.search(table="population", namespace="un")
    dataset = client.datasets.fetch("garden/un/2024/pop/pop")  # Fetch metadata
    table = results[0].load()
    table = client.datasets["garden/un/2024/population/population"]  # Direct access

    # Advanced: Search pages/articles
    pages = client._site_search.pages("climate change")
    ```
"""

from __future__ import annotations

from .charts import ChartNotFoundError, ChartsAPI, LicenseError
from .datasets import DatasetsAPI
from .indicators import IndicatorsAPI
from .models import (
    ChartResult,
    DatasetResult,
    IndicatorResult,
    PageSearchResult,
    ResultSet,
)
from .search import SiteSearchAPI


class Client:
    """Unified client for all OWID data APIs.

    Provides access to four main APIs:

    - charts: Fetch and search for published charts
    - indicators: Semantic search for data indicators
    - datasets: Query and load from the data catalog
    - _site_search: Internal site search (prefer using charts.search() or indicators.search())

    Attributes:
        charts: ChartsAPI instance for chart operations and search.
        indicators: IndicatorsAPI instance for indicator search.
        datasets: DatasetsAPI instance for catalog operations.

    Example:
        ```python
        from owid.catalog.client import Client

        # Create client (recommended: reuse for multiple operations)
        client = Client()

        # Charts API
        df = client.charts.get_data("life-expectancy")
        chart = client.charts.fetch("gdp")  # Fetch specific chart
        results = client.charts.search("population")  # Search for charts

        # Indicators API
        indicators = client.indicators.search("solar energy")
        indicator = client.indicators.fetch(12345)  # Fetch by ID

        # Datasets API
        results = client.datasets.search(table="population", namespace="un")
        dataset = client.datasets.fetch("garden/un/2024/pop/pop")  # Fetch metadata
        table = results[0].load()  # Load data
        ```
    """

    charts: ChartsAPI
    indicators: IndicatorsAPI
    datasets: DatasetsAPI
    _site_search: SiteSearchAPI

    def __init__(self) -> None:
        """Initialize the client with all API interfaces."""
        self.charts = ChartsAPI(self)
        self.indicators = IndicatorsAPI(self)
        self.datasets = DatasetsAPI(self)
        self._site_search = SiteSearchAPI(self)

    def __repr__(self) -> str:
        return "Client(charts=..., indicators=..., datasets=...)"


__all__ = [
    # Main client
    "Client",
    # Result types for type hints
    "ChartResult",
    "PageSearchResult",
    "IndicatorResult",
    "DatasetResult",
    "ResultSet",
    # Exceptions for error handling
    "ChartNotFoundError",
    "LicenseError",
]
