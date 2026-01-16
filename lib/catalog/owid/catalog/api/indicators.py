#
#  owid.catalog.client.indicators
#
#  Indicators API for semantic search of OWID indicators.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import requests
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from owid.catalog.api.models import ResponseSet
from owid.catalog.api.tables import _load_table
from owid.catalog.core import CatalogPath
from owid.catalog.tables import Table

if TYPE_CHECKING:
    from owid.catalog.api import Client


# =============================================================================
# Indicator Loading Functions
# =============================================================================


def _load_indicator(
    path: str,
    *,
    catalog_url: str,
    load_data: bool = True,
) -> Table:
    """Load indicator data by catalog path.

    Shared logic for IndicatorResult.fetch() and IndicatorsAPI.fetch().

    Args:
        path: Catalog path in format "channel/namespace/version/dataset/table#column"
        catalog_url: Base URL for the catalog (required).
        load_data: If True, load full data. If False, load only structure.

    Returns:
        Table with a single indicator column (plus index).

    Raises:
        ValueError: If path format is invalid or column doesn't exist.
    """
    # Parse path
    catalog_path = CatalogPath.from_str(path)

    if not catalog_path.variable:
        raise ValueError(
            f"Invalid indicator path format: '{path}'. "
            "Expected format: 'channel/namespace/version/dataset/table#column' (missing #column)"
        )

    table_path = catalog_path.table_path
    column_name = catalog_path.variable

    if table_path is None:
        raise ValueError(
            f"Invalid indicator path format: '{path}'. "
            "Expected format: 'channel/namespace/version/dataset/table#column'"
        )

    # Load table
    table = _load_table(table_path, load_data=load_data, catalog_url=catalog_url)

    # Validate column exists
    if column_name not in table.columns:
        available_cols = ", ".join(f"'{col}'" for col in sorted(table.columns)[:10])
        total_cols = len(table.columns)
        cols_display = f"{available_cols}{'...' if total_cols > 10 else ''}"
        raise ValueError(
            f"Column '{column_name}' not found in table '{table_path}'. "
            f"Available columns ({total_cols}): {cols_display}"
        )

    return table.loc[:, [column_name]]


# =============================================================================
# IndicatorResult Model
# =============================================================================


class IndicatorResult(BaseModel):
    """An indicator found via semantic search.

    Attributes:
        title: Indicator title/name.
        indicator_id: Unique indicator ID.
        path: Path in the catalog (e.g., "grapher/un/2024-07-12/un_wpp/population#population").
        channel: Data channel (parsed from path).
        namespace: Data provider namespace (parsed from path).
        version: Version string (parsed from path).
        dataset: Dataset name (parsed from path).
        column_name: Column name in the table.
        description: Full indicator description.
        unit: Unit of measurement.
        score: Semantic similarity score (0-1).
        n_charts: Number of charts using this indicator.
        popularity: Popularity score (0.0 to 1.0) based on analytics views.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    # Identification
    title: str
    indicator_id: int | None

    # Location
    path: str | None

    # Structural metadata
    channel: str | None = None
    namespace: str | None = None
    version: str | None = None
    dataset: str | None = None

    # Content metadata
    column_name: str = ""
    description: str = ""
    unit: str = ""

    # Usage metadata
    score: float
    n_charts: int | None = None
    popularity: float = 0.0

    # API configuration (immutable)
    catalog_url: str = Field(frozen=True)

    _cached_table: Table | None = PrivateAttr(default=None)
    _legacy: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        """Parse dataset, version, namespace, channel from path."""
        if self.path and not self.dataset:
            # Parse using CatalogPath
            try:
                # CatalogPath.from_str() handles the "#" automatically
                parsed = CatalogPath.from_str(self.path)
                # Set parsed fields
                object.__setattr__(self, "dataset", parsed.dataset)
                object.__setattr__(self, "version", parsed.version)
                object.__setattr__(self, "namespace", parsed.namespace)
                object.__setattr__(self, "channel", parsed.channel)
            except Exception:
                # If parsing fails, leave fields empty
                object.__setattr__(self, "_legacy", True)

    def fetch(self, *, load_data: bool = True) -> Table:
        """Fetch indicator data as a single-column Table.

        Args:
            load_data: If True (default), load full indicator data.
                       If False, load only structure (columns and metadata) without rows.

        Returns:
            Table with the indicator column (plus index). Metadata is preserved.

        Example:
            ```python
            result = client.indicators.search("population")[0]
            tb = result.fetch()
            print(tb.head())
            print(tb[tb.columns[0]].metadata.unit)
            ```
        """
        if self.path is None:
            raise ValueError("Cannot fetch: path is None. Likely a legacy (pre-ETL) indicator.")

        # Return cached if available and requesting full data
        if load_data and self._cached_table is not None:
            return self._cached_table

        tb = _load_indicator(self.path, load_data=load_data, catalog_url=self.catalog_url)

        # Cache only if loading full data
        if load_data:
            self._cached_table = tb

        return tb

    def fetch_table(self, *, load_data: bool = True) -> Table:
        """Fetch the full table containing this indicator.

        Args:
            load_data: If True (default), load full table data.
                       If False, load only structure (columns and metadata) without rows.

        Returns:
            Table with all columns including this indicator.

        Example:
            ```python
            result = client.indicators.search("population")[0]
            tb = result.fetch_table()
            print(tb.columns)
            ```
        """
        # Return cached if available and requesting full data
        if load_data and self._cached_table is not None:
            return self._cached_table

        result = self._load_full_table(load_data=load_data)

        # Cache only if loading full data
        if load_data:
            self._cached_table = result

        return result

    def _load_full_table(self, *, load_data: bool = True) -> Table:
        """Internal method to load the table containing this indicator."""
        # Check path is not None
        if self.path is None:
            raise ValueError("Cannot load table: path is None. Likely because this is a legacy (pre-ETL) table.")
        # Parse path using CatalogPath
        parsed = CatalogPath.from_str(self.path)
        # Use table_path property (without variable)
        if parsed.table_path is None:
            raise ValueError(f"Invalid catalog path: {self.path}")
        return _load_table(parsed.table_path, load_data=load_data, catalog_url=self.catalog_url)


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

        # Fetch the indicator data as a single-column Table
        tb = results[0].fetch()

        # Or fetch the full table containing the indicator
        full_table = results[0].fetch_table()
        ```
    """

    def __init__(self, client: "Client", search_url: str, catalog_url: str) -> None:
        """Initialize the IndicatorsAPI.

        Args:
            client: The Client instance.
            search_url: URL for the indicators search API (e.g., "https://search.owid.io/indicators").
            catalog_url: Base URL for the catalog (e.g., "https://catalog.ourworldindata.org/").
        """
        self._client = client
        self._search_url = search_url
        self._catalog_url = catalog_url

    @property
    def search_url(self) -> str:
        """URL for the indicators search API (read-only)."""
        return self._search_url

    @property
    def catalog_url(self) -> str:
        """Base URL for the catalog (read-only)."""
        return self._catalog_url

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
            SearchResults containing IndicatorResult objects, sorted by semantic similarity score.
            Each result includes a `popularity` field (0.0-1.0) based on analytics views.

        Example:
            ```python
            # Search for indicators
            results = client.indicators.search("CO2 emissions per capita")

            # View results (sorted by semantic score)
            for ind in results:
                print(f"{ind.title}")
                print(f"  Score: {ind.score:.3f}")
                print(f"  Popularity: {ind.popularity:.3f}")

            # Load data from top result
            tb = results[0].fetch()

            # Re-sort by popularity if desired
            by_popularity = results.sort_by('popularity', reverse=True)
            ```
        """
        params = {
            "query": query,
            "limit": limit,
        }

        resp = requests.get(self.search_url, params=params, timeout=timeout or self._client.timeout)
        resp.raise_for_status()
        data = resp.json()

        raw_results: list[tuple[dict[str, Any], str | None]] = []
        for r in data.get("results", []):
            path = r.get("catalog_path", "")

            # If legacy indicator, keep only if asked to
            if "/NULL/" in path:
                if not show_legacy:
                    # Skip legacy indicators unless requested
                    continue
                path = None
            raw_results.append((r, path))

        # Fetch popularity via Datasette API
        slugs = [path for _, path in raw_results if path]
        popularity_data = (
            self._client._datasette.fetch_popularity(
                slugs,
                type="indicator",
                timeout=timeout or self._client.timeout,
            )
            if slugs
            else {}
        )

        results = [
            IndicatorResult(
                indicator_id=r.get("indicator_id", 0),
                title=r.get("title", ""),
                score=r.get("score", 0.0),
                path=path,
                description=r.get("description", ""),
                column_name=r.get("metadata", {}).get("column", ""),
                unit=r.get("metadata", {}).get("unit", ""),
                n_charts=r.get("n_charts", 0),
                popularity=popularity_data.get(path, 0.0) if path else 0.0,
                catalog_url=self.catalog_url,
            )
            for r, path in raw_results
        ]

        return ResponseSet(
            results=results,
            query=query,
            total_count=data.get("total_results", len(results)),
            base_url=self.catalog_url,
        )

    def fetch(self, path: str, *, load_data: bool = True, timeout: int | None = None) -> Table:
        """Fetch a specific indicator by catalog path.

        Args:
            path: Catalog path in format "channel/namespace/version/dataset/table#column"
            load_data: If True (default), load full indicator data.
                       If False, load only structure (columns and metadata) without rows.
            timeout: HTTP request timeout in seconds (reserved for future use).

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
        _ = timeout  # Reserved for future use
        return _load_indicator(path, load_data=load_data, catalog_url=self.catalog_url)
