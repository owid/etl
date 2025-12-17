#
#  owid.catalog.client.search
#
#  Search API for finding charts and pages via Algolia.
#
from __future__ import annotations

from typing import TYPE_CHECKING

import requests

from .models import ChartSearchResult, PageSearchResult, ResultSet

if TYPE_CHECKING:
    from . import Client


class SearchAPI:
    """API for searching OWID charts and pages.

    Uses the Algolia-powered search endpoint to find content
    on ourworldindata.org.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Search for charts
        results = client.search.charts("gdp per capita")
        for chart in results:
            print(chart.title)

        # Search for pages/articles
        results = client.search.pages("climate change")
        for page in results:
            print(page.title)
        ```
    """

    BASE_URL = "https://ourworldindata.org/api/search"

    def __init__(self, client: "Client") -> None:
        self._client = client

    def charts(
        self,
        query: str,
        *,
        countries: list[str] | None = None,
        topics: list[str] | None = None,
        require_all_countries: bool = False,
        limit: int = 20,
        page: int = 0,
    ) -> ResultSet[ChartSearchResult]:
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
            SearchResults containing ChartSearchResult objects.

        Example:
            ```python
            # Basic search
            results = client.search.charts("life expectancy")

            # Filter by countries
            results = client.search.charts(
                "gdp",
                countries=["France", "Germany"],
                require_all_countries=True
            )

            # Paginate results
            page1 = client.search.charts("population", limit=10, page=0)
            page2 = client.search.charts("population", limit=10, page=1)
            ```
        """
        params: dict = {
            "q": query,
            "type": "charts",
            "hitsPerPage": min(max(1, limit), 100),
            "page": page,
        }

        if countries:
            params["countries"] = "~".join(countries)
        if topics:
            params["topics"] = "~".join(topics)
        if require_all_countries:
            params["requireAllCountries"] = "true"

        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for hit in data.get("results", []):
            slug = hit.get("slug", "")
            results.append(
                ChartSearchResult(
                    slug=slug,
                    title=hit.get("title", ""),
                    url=f"https://ourworldindata.org/grapher/{slug}",
                    subtitle=hit.get("subtitle", "") or hit.get("variantName", ""),
                    available_entities=hit.get("availableEntities", []),
                    num_related_articles=hit.get("numRelatedArticles", 0),
                )
            )

        return ResultSet(
            results=results,
            query=query,
            total=data.get("totalCount", len(results)),
        )

    def pages(
        self,
        query: str,
        *,
        limit: int = 20,
        page: int = 0,
    ) -> ResultSet[PageSearchResult]:
        """Search for pages/articles matching a query.

        Args:
            query: Search query string.
            limit: Maximum results to return (1-100). Default 20.
            page: Page number for pagination (0-indexed). Default 0.

        Returns:
            SearchResults containing PageSearchResult objects.

        Example:
            ```python
            results = client.search.pages("climate change")
            for page in results:
                print(f"{page.title}: {page.url}")
            ```
        """
        params: dict = {
            "q": query,
            "type": "pages",
            "hitsPerPage": min(max(1, limit), 100),
            "page": page,
        }

        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for hit in data.get("results", []):
            slug = hit.get("slug", "")
            results.append(
                PageSearchResult(
                    slug=slug,
                    title=hit.get("title", ""),
                    url=f"https://ourworldindata.org/{slug}",
                    excerpt=hit.get("excerpt", ""),
                    authors=hit.get("authors", []),
                    published_at=hit.get("publishedAt", ""),
                    thumbnail_url=hit.get("thumbnailUrl", ""),
                )
            )

        return ResultSet(
            results=results,
            query=query,
            total=data.get("totalCount", len(results)),
        )
