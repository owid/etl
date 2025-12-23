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
    from owid.catalog.variables import Variable


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

        # Get indicator data directly by URI
        variable = client.indicators.get_data("garden/un/2024-07-12/un_wpp/population#population")

        # Or fetch indicator metadata first
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
            indicator = results[0].data
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
                    column_name=r.get("metadata", {}).get("column", ""),
                    unit=r.get("metadata", {}).get("unit", ""),
                    n_charts=r.get("n_charts", 0),
                )
            )

        return ResultSet(
            items=results,
            query=query,
            total=data.get("total_results", len(results)),
        )

    def fetch(self, path: str, *, load_data: bool = False) -> IndicatorResult:
        """Fetch a specific indicator by catalog path.

        Args:
            path: Catalog path in format "channel/namespace/version/dataset/table#column"
                  (e.g., "grapher/un/2024/pop/population#population_total")
            load_data: If True, preload indicator data immediately.
                       If False (default), data is loaded lazily when accessed via .data property.

        Returns:
            IndicatorResult with full details. Access .data to get the variable.

        Raises:
            ValueError: If path format is invalid, table not found, or column doesn't exist.

        Example:
            ```python
            # Fetch indicator by URI
            indicator = client.indicators.fetch("grapher/un/2024/pop/population#population_total")
            print(f"Title: {indicator.title}")
            variable = indicator.data
            ```

        Note:
            Known limitations of this method:
            - Uses TablesAPI internally, loading the full table to access a single column
            - No indicator_id is assigned when fetching by URI (returns None)
            - Future optimization may allow direct indicator access without full table loading
        """
        # Parse path to extract table_path and column_name
        if "#" not in path:
            raise ValueError(
                f"Invalid indicator path format: '{path}'. "
                "Expected format: 'channel/namespace/version/dataset/table#column'"
            )

        table_path, _, column_name = path.partition("#")

        if not table_path or not column_name:
            raise ValueError(
                f"Invalid indicator path format: '{path}'. "
                "Both table path and column name (separated by #) are required."
            )

        # Fetch table header (structure only, no rows) to validate column and extract metadata
        try:
            table_result = self._client.tables.fetch(table_path)
            # Load only the header (columns and metadata, no data rows)
            table = table_result.data_header
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

        # Extract metadata from the Variable object
        metadata = table[column_name].metadata

        # Create IndicatorResult
        indicator = IndicatorResult(
            indicator_id=None,  # TODO: No ID when fetching by URI. We should provide one.
            title=metadata.title or column_name,
            score=1.0,  # Direct fetch - perfect match
            catalog_path=path,
            description=metadata.description or "",
            column_name=column_name,
            unit=metadata.unit or "",
            n_charts=None,  # TODO: Not available when fetched by URI.
        )

        # Don't store the table - let it be loaded lazily when indicator.data is accessed
        # The table will be loaded fresh via indicator._load_table() when needed

        # Preload data if requested
        if load_data:
            _ = indicator.data

        return indicator

    def get_data(self, path: str) -> "Variable":
        """Fetch indicator data as a Variable.

        Convenience method equivalent to fetch(path).data

        Args:
            path: Catalog path in format "channel/namespace/version/dataset/table#column"
                  (e.g., "grapher/un/2024/pop/population#population_total")

        Returns:
            Variable (pandas Series with metadata).

        Raises:
            ValueError: If path format is invalid, table not found, or column doesn't exist.

        Example:
            ```python
            variable = client.indicators.get_data("garden/un/2024-07-12/un_wpp/population#population")
            print(variable.head())
            print(variable.metadata.unit)
            ```
        """
        return self.fetch(path).data
