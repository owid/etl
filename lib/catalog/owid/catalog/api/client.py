from owid.catalog.api.charts import ChartsAPI
from owid.catalog.api.datasette import DatasetteAPI
from owid.catalog.api.indicators import IndicatorsAPI
from owid.catalog.api.search import SiteSearchAPI
from owid.catalog.api.tables import TablesAPI
from owid.catalog.api.utils import (
    DEFAULT_CATALOG_URL,
    DEFAULT_GRAPHER_URL,
    DEFAULT_INDICATORS_SEARCH_URL,
    DEFAULT_SITE_SEARCH_URL,
)


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
        tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")

        # Indicators: Semantic search for data series
        results = client.indicators.search("renewable energy")
        variable = client.indicators.fetch("garden/un/2024-07-12/un_wpp/population#population")

        # Custom URLs (e.g., for staging environments)
        staging_client = Client(catalog_url="https://staging-catalog.example.com/")
        ```
    """

    charts: ChartsAPI
    indicators: IndicatorsAPI
    tables: TablesAPI
    timeout: int
    _datasette: DatasetteAPI
    _site_search: SiteSearchAPI

    def __init__(
        self,
        timeout: int = 30,
        catalog_url: str = DEFAULT_CATALOG_URL,
        grapher_url: str = DEFAULT_GRAPHER_URL,
        indicators_search_url: str = DEFAULT_INDICATORS_SEARCH_URL,
        site_search_url: str = DEFAULT_SITE_SEARCH_URL,
    ) -> None:
        """Initialize the client with all API interfaces.

        Args:
            timeout: HTTP request timeout in seconds. Default 30.
            catalog_url: Base URL for the catalog. Default: https://catalog.ourworldindata.org/
            grapher_url: Base URL for the Grapher. Default: https://ourworldindata.org/grapher
            indicators_search_url: URL for indicators search API. Default: https://search.owid.io/indicators
            site_search_url: URL for site search API. Default: https://ourworldindata.org/api/search
        """
        self.timeout = timeout
        self._datasette = DatasetteAPI(timeout=timeout)
        self.charts = ChartsAPI(self, base_url=grapher_url)
        self.indicators = IndicatorsAPI(self, search_url=indicators_search_url, catalog_url=catalog_url)
        self.tables = TablesAPI(self, catalog_url=catalog_url)
        self._site_search = SiteSearchAPI(self, base_url=site_search_url, grapher_url=grapher_url)

    def __repr__(self) -> str:
        return "Client(charts=..., indicators=..., tables=...)"
