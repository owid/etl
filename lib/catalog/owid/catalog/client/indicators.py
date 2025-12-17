#
#  owid.catalog.client.indicators
#
#  Indicators API for semantic search of OWID indicators.
#
from __future__ import annotations

from typing import TYPE_CHECKING

import requests

from .models import IndicatorResult, ResultSet

if TYPE_CHECKING:
    from ..tables import Table
    from . import Client


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

        # Load the top result's table
        table = results[0].load()
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

    def get(self, indicator_id: int) -> "Table":
        """Get an indicator by its ID.

        Fetches the table containing the indicator from the catalog.

        Args:
            indicator_id: The indicator's unique ID.

        Returns:
            Table containing the indicator data.

        Note:
            This method requires knowing the indicator ID. To discover
            indicators, use the search() method first.
        """
        # First search to find the catalog path
        # This is a workaround since we don't have a direct ID lookup endpoint
        params = {
            "query": str(indicator_id),
            "limit": 100,
        }

        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        for r in data.get("results", []):
            if r.get("indicator_id") == indicator_id:
                path_part, _, _ = r.get("catalog_path", "").partition("#")
                if path_part:
                    from .datasets import DatasetsAPI

                    return DatasetsAPI._load_table(path_part)

        raise ValueError(f"Indicator with ID {indicator_id} not found")
