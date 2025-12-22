#
#  owid.catalog.client.indicators
#
#  Indicators API for semantic search of OWID indicators.
#
from __future__ import annotations

from typing import TYPE_CHECKING

import requests

from owid.catalog.api.models import IndicatorResult, ResultSet

if TYPE_CHECKING:
    from owid.catalog.api import Client


class IndicatorsAPI:
    """API for semantic search of OWID indicators.

    Uses the search.owid.io service to find indicators using
    natural language queries and vector embeddings.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Search for indicators
        results = client.indicators.search("solar power generation")
        for ind in results:
            print(f"{ind.title} (score: {ind.score:.2f})")

        # Load the table that contains the indicator of interest
        table = results[0].load()

        # Fetch specific indicator by ID
        indicator = client.indicators.fetch(12345)
        table = indicator.load()
        ```
    """

    BASE_URL = "https://search.owid.io/indicators"

    def __init__(self, client: "Client") -> None:
        self._client = client

    def search(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> ResultSet[IndicatorResult]:
        """Search for indicators using natural language.

        Uses semantic search to find indicators that match the
        meaning of your query, not just keyword matching.

        Args:
            query: Natural language search query
                (e.g., "renewable energy capacity", "child mortality rate").
            limit: Maximum number of results to return. Default 10.

        Returns:
            SearchResults containing IndicatorResult objects.

        Example:
            ```python
            # Search for indicators
            results = client.indicators.search("CO2 emissions per capita")

            # View results
            for ind in results:
                print(f"{ind.title}")
                print(f"  Score: {ind.score:.3f}")
                print(f"  Path: {ind.catalog_path}")

            # Load data from top result
            table = results[0].load()
            ```
        """
        params = {
            "query": query,
            "limit": limit,
        }

        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("results", []):
            results.append(
                IndicatorResult(
                    indicator_id=r.get("indicator_id", 0),
                    title=r.get("title", ""),
                    score=r.get("score", 0.0),
                    catalog_path=r.get("catalog_path", ""),
                    description=r.get("description", ""),
                    column_name=r.get("metadata", {}).get("column_name", ""),
                    unit=r.get("metadata", {}).get("unit", ""),
                    n_charts=r.get("n_charts", 0),
                )
            )

        return ResultSet(
            results=results,
            query=query,
            total=data.get("total_results", len(results)),
        )

    def fetch(self, indicator_id: int, *, load_data: bool = False) -> IndicatorResult:
        """Fetch a specific indicator by ID.

        Args:
            indicator_id: Unique indicator ID.
            load_data: If True, preload indicator data immediately.
                       If False (default), data is loaded lazily when accessed via .data property.

        Returns:
            IndicatorResult with full details. Access .data to get the variable.

        Raises:
            ValueError: If indicator not found.

        Example:
            ```python
            indicator = client.indicators.fetch(12345)
            print(f"Title: {indicator.title}")
            variable = indicator.data  # Lazy-loaded Variable

            # Or preload data immediately
            indicator = client.indicators.fetch(12345, load_data=True)
            variable = indicator.data  # Already loaded
            ```
        """
        # Search by ID to find it
        results = self.search(str(indicator_id), limit=100)
        for r in results:
            if r.indicator_id == indicator_id:
                # Preload data if requested
                if load_data:
                    _ = r.data  # Access property to trigger loading
                return r
        raise ValueError(f"Indicator {indicator_id} not found")
