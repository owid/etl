#
#  owid.catalog.client.indicators
#
#  Indicators API for semantic search of OWID indicators.
#
from __future__ import annotations

from typing import TYPE_CHECKING

import requests

from owid.catalog.api.models import IndicatorResult, ResponseSet
from owid.catalog.core import CatalogPath
from owid.catalog.tables import Table

if TYPE_CHECKING:
    from owid.catalog.api import Client


OWID_SEARCH_API = "https://search.owid.io/indicators"


class IndicatorsAPI:
    """API for semantic search of OWID indicators.

    Uses the search.owid.io service to find indicators using
    natural language queries and vector embeddings.

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Search for indicators
        results = client.indicators.search("solar power generation")
        for ind in results:
            print(f"{ind.title} (score: {ind.score:.2f})")

        # Load the table that contains the indicator of interest
        table = results[0].table

        # Fetch indicator metadata first, then access data
        indicator = client.indicators.fetch("garden/un/2024-07-12/un_wpp/population#population")
        variable = indicator.data
        ```
    """

    BASE_URL = OWID_SEARCH_API

    def __init__(self, client: "Client") -> None:
        """Initialize the IndicatorsAPI.

        Args:
            client: The Client instance.
        """
        self._client = client

    def search(
        self,
        query: str,
        *,
        limit: int = 10,
        show_legacy: bool = False,
        timeout: int | None = None,
    ) -> ResponseSet[IndicatorResult]:
        """Search for indicators using natural language.

        Uses semantic search to find indicators that match the
        meaning of your query, not just keyword matching.

        Args:
            query: Natural language search query
                (e.g., "renewable energy capacity", "child mortality rate").
            limit: Maximum number of results to return. Default 10.
            show_legacy: If True, show pre-ETL indicators only. Default False.
            timeout: HTTP request timeout in seconds. Defaults to client timeout.

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
                print(f"  Path: {ind.path}")

            # Load data from top result
            indicator = results[0].data
            ```
        """
        params = {
            "query": query,
            "limit": limit,
        }

        resp = requests.get(self.BASE_URL, params=params, timeout=timeout or self._client.timeout)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("results", []):
            path = r.get("catalog_path", "")

            # If legacy indicator, keep only if asked to
            if "/NULL/" in path:
                if not show_legacy:
                    # Skip legacy indicators unless requested
                    continue
                path = None
            results.append(
                IndicatorResult(
                    indicator_id=r.get("indicator_id", 0),
                    title=r.get("title", ""),
                    score=r.get("score", 0.0),
                    path=path,
                    description=r.get("description", ""),
                    column_name=r.get("metadata", {}).get("column", ""),
                    unit=r.get("metadata", {}).get("unit", ""),
                    n_charts=r.get("n_charts", 0),
                )
            )

        return ResponseSet(
            results=results,
            query=query,
            total_count=data.get("total_results", len(results)),
        )

    def fetch(self, path: str, *, load_data: bool = True, timeout: int | None = None) -> Table:
        """Fetch a specific indicator by catalog path.

        Args:
            path: Catalog path in format "channel/namespace/version/dataset/table#column"
                  (e.g., "garden/un/2024/pop/population#population_total")
            load_data: If True (default), load full indicator data.
                       If False, load only structure (columns and metadata) without rows.
            timeout: HTTP request timeout in seconds. Defaults to client timeout.

        Returns:
            Table with a single indicator column (plus index). Metadata is preserved.

        Raises:
            ValueError: If path format is invalid, table not found, or column doesn't exist.

        Example:
            ```python
            # Fetch indicator by path
            tb = client.indicators.fetch("garden/un/2024-07-12/un_wpp/population#population")
            print(tb.head())
            print(tb["population"].metadata.unit)
            ```
        """
        effective_timeout = timeout or self._client.timeout

        # Parse path using CatalogPath
        catalog_path = CatalogPath.from_str(path)

        if not catalog_path.variable:
            raise ValueError(
                f"Invalid indicator path format: '{path}'. "
                "Expected format: 'channel/namespace/version/dataset/table#column' (missing #column)"
            )

        # Build table path (without variable)
        table_path = catalog_path.table_path
        column_name = catalog_path.variable

        if table_path is None:
            raise ValueError(
                f"Invalid indicator path format: '{path}'. "
                "Expected format: 'channel/namespace/version/dataset/table#column'"
            )

        # Fetch full table
        try:
            table = self._client.tables.fetch(table_path, load_data=load_data, timeout=effective_timeout)
        except Exception as e:
            raise ValueError(f"Failed to load table at path: '{table_path}'. Error: {e}") from e

        # Verify column exists in table
        if column_name not in table.columns:
            available_cols = ", ".join(f"'{col}'" for col in sorted(table.columns)[:10])
            total_cols = len(table.columns)
            cols_display = f"{available_cols}{'...' if total_cols > 10 else ''}"

            raise ValueError(
                f"Column '{column_name}' not found in table '{table_path}'. "
                f"Available columns ({total_cols}): {cols_display}"
            )

        # Extract just this indicator column as a single-column Table
        # This preserves the index and column metadata
        return table.loc[:, [column_name]]
