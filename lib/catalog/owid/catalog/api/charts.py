#
#  owid.catalog.client.charts
#
#  Charts API for fetching data from published OWID charts.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
import requests

from owid.catalog.api.models import ChartNotFoundError, ChartResult, ResponseSet

if TYPE_CHECKING:
    from owid.catalog.api import Client


class ChartsAPI:
    """API for accessing OWID chart data and metadata.

    Provides methods to fetch data, metadata, and configuration from
    published charts on ourworldindata.org. Also includes search
    functionality to find charts by keywords.

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Get chart data directly
        df = client.charts.get_data("life-expectancy")

        # Or fetch chart and access data via property
        chart = client.charts.fetch("life-expectancy")
        df = chart.data  # Lazy-loaded via property

        # Search for charts
        results = client.charts.search("gdp per capita")
        df = results[0].data  # Access data via property

        # Get chart metadata
        meta = client.charts.get_metadata("life-expectancy")
        ```
    """

    BASE_URL = "https://ourworldindata.org/grapher"

    def __init__(self, client: "Client") -> None:
        self._client = client

    def search(
        self,
        query: str,
        *,
        countries: list[str] | None = None,
        topics: list[str] | None = None,
        require_all_countries: bool = False,
        limit: int = 20,
        page: int = 0,
    ) -> ResponseSet[ChartResult]:
        """Search for charts matching a query.

        Args:
            query: Search query string.
            countries: Optional list of country names to filter by.
            topics: Optional list of topic names to filter by.
            require_all_countries: If True, only return charts with ALL
                specified countries. Default False (any country matches).
            limit: Maximum results to return (1-100). Default 20.
            page: Page number for pagination (0-indexed). Default 0.

        Returns:
            ResponseSet containing ChartResult objects.

        Example:
            ```python
            # Basic search
            results = client.charts.search("life expectancy")
            for chart in results:
                print(chart.title)

            # Filter by countries
            results = client.charts.search(
                "gdp",
                countries=["France", "Germany"],
                require_all_countries=True
            )

            # Get data from search results
            df = results[0].data
            ```
        """
        return self._client._site_search.charts(
            query=query,
            countries=countries,
            topics=topics,
            require_all_countries=require_all_countries,
            limit=limit,
            page=page,
        )

    def fetch(self, slug_or_url: str, *, load_data: bool = False) -> ChartResult:
        """Fetch a chart with all its metadata and config.

        Args:
            slug_or_url: Chart slug or full URL.
            load_data: If True, preload chart data immediately.
                       If False (default), data is loaded lazily when accessed via .data property.

        Returns:
            ChartResult with metadata, config, and data loading capability.

        Example:
            ```python
            chart = client.charts.fetch("life-expectancy")
            print(chart.title)
            df = chart.data
            ```
        """
        slug = self._parse_slug(slug_or_url)
        config = self._fetch_config(slug)
        metadata = self._fetch_metadata(slug)

        result = ChartResult(
            slug=slug,
            title=config.get("title", ""),
            url=f"{self.BASE_URL}/{slug}",
            config=config,
            metadata=metadata,
        )

        # Preload data if requested
        if load_data:
            _ = result.data  # Access property to trigger loading

        return result

    def get_data(self, slug_or_url: str) -> pd.DataFrame:
        """Fetch chart data as a DataFrame.

        Args:
            slug_or_url: Chart slug (e.g., "life-expectancy") or full URL.

        Returns:
            DataFrame with chart data. Additional metadata in df.attrs.

        Raises:
            ChartNotFoundError: If the chart does not exist.
            LicenseError: If the data cannot be downloaded due to licensing.

        Example:
            ```python
            df = client.charts.get_data("life-expectancy")
            print(df.head())
            ```
        """
        # Use fetch() to get ChartResult, then access .data property
        return self.fetch(slug_or_url).data

    def get_metadata(self, slug_or_url: str) -> dict[str, Any]:
        """Fetch chart metadata.

        Args:
            slug_or_url: Chart slug or full URL.

        Returns:
            Dict containing chart metadata including column information.

        Example:
            ```python
            meta = client.charts.get_metadata("life-expectancy")
            print(meta["columns"].keys())
            ```
        """
        slug = self._parse_slug(slug_or_url)
        return self._fetch_metadata(slug)

    def get_config(self, slug_or_url: str) -> dict[str, Any]:
        """Fetch raw grapher configuration.

        Args:
            slug_or_url: Chart slug or full URL.

        Returns:
            Dict containing the grapher configuration.

        Example:
            ```python
            config = client.charts.get_config("life-expectancy")
            print(config["title"])
            ```
        """
        slug = self._parse_slug(slug_or_url)
        return self._fetch_config(slug)

    @staticmethod
    def _parse_slug(slug_or_url: str) -> str:
        """Extract slug from URL or return as-is."""
        if slug_or_url.startswith("https://ourworldindata.org/grapher/"):
            # Handle URLs with query params
            path = slug_or_url.split("/grapher/")[1]
            return path.split("?")[0]
        elif slug_or_url.startswith("https://"):
            raise ValueError("URL must be a Grapher URL, e.g. https://ourworldindata.org/grapher/life-expectancy")
        return slug_or_url

    @staticmethod
    def _fetch_metadata(slug: str) -> dict[str, Any]:
        """Fetch metadata JSON from a chart."""
        url = f"{ChartsAPI.BASE_URL}/{slug}.metadata.json"
        resp = requests.get(url)

        if resp.status_code == 404:
            raise ChartNotFoundError(f"No such chart found: {slug}")

        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _fetch_config(slug: str) -> dict[str, Any]:
        """Fetch config JSON from a chart."""
        url = f"{ChartsAPI.BASE_URL}/{slug}.config.json"
        resp = requests.get(url)

        if resp.status_code == 404:
            raise ChartNotFoundError(f"No such chart found: {slug}")

        resp.raise_for_status()
        return resp.json()
