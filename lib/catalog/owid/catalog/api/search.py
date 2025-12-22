#
#  owid.catalog.client.search
#
#  Site search API for finding charts and pages via Algolia.
#
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import requests

from owid.catalog.api.models import ChartResult, PageSearchResult, ResultSet

if TYPE_CHECKING:
    from owid.catalog.api import Client


class SiteSearchAPI:
    """API for searching the OWID website (charts and pages).

    Uses the Algolia-powered search endpoint to find content
    on ourworldindata.org.

    Note:
        For searching charts specifically, prefer using `client.charts.search()`.
        This API is intended for advanced usage or when you need to search pages.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Search for pages/articles
        results = client._site_search.pages("climate change")
        for page in results:
            print(page.title)

        # For charts, prefer: client.charts.search("gdp")
        ```
    """

    BASE_URL = "https://ourworldindata.org/api/search"

    def __init__(self, client: "Client") -> None:
        self._client = client

    def _search(
        self,
        query: str,
        type_: str,
        limit: int,
        page: int,
        extra_params: dict | None = None,
    ) -> dict:
        """Internal method to perform search request with common logic."""
        # Warn if limit exceeds maximum
        if limit > 100:
            warnings.warn(
                "Max allowed of result items is 100. Using limit=100 instead.",
                UserWarning,
                stacklevel=3,  # stacklevel=3 to point to the public method caller
            )

        params: dict = {
            "q": query,
            "type": type_,
            "hitsPerPage": min(max(1, limit), 100),
            "page": page,
        }

        if extra_params:
            params.update(extra_params)

        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        return resp.json()

    def charts(
        self,
        query: str,
        *,
        countries: list[str] | None = None,
        topics: list[str] | None = None,
        require_all_countries: bool = False,
        limit: int = 20,
        page: int = 0,
    ) -> ResultSet[ChartResult]:
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
            SearchResults containing ChartResult objects.

        Note:
            Prefer using `client.charts.search()` for a simpler API.

        Example:
            ```python
            # Recommended: use client.charts.search()
            results = client.charts.search("life expectancy")

            # Advanced: use site search directly
            results = client._site_search.charts("gdp", countries=["France"])
            ```
        """
        # Build extra parameters for chart-specific filters
        extra_params: dict = {}
        if countries:
            extra_params["countries"] = "~".join(countries)
        if topics:
            extra_params["topics"] = "~".join(topics)
        if require_all_countries:
            extra_params["requireAllCountries"] = "true"

        # Perform search
        data = self._search(query, "charts", limit, page, extra_params)

        # Parse results
        results = []
        for hit in data.get("results", []):
            slug = hit.get("slug", "")
            results.append(
                ChartResult(
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
            results = client._site_search.pages("climate change")
            for page in results:
                print(f"{page.title}: {page.url}")
            ```
        """
        # Perform search
        data = self._search(query, "pages", limit, page)

        # Parse results
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
