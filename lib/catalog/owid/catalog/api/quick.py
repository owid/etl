"""Quick access functions for data discovery and retrieval. For more complex use cases, refer to the [full API](../#owid.catalog.api.Client).

This module provides convenience functions for discovering and accessing OWID catalog data.

The API separates discovery (searching)
Example: Search for available data (no download)
    ```python
    >>> from owid.catalog import search
    >>> results = search("population")  # Returns ResponseSet[TableResult]
    >>> print(f"Found {len(results)} tables")
    >>> print(results[0].path)
    ```

from download (fetching)

Example: Fetch specific data by path
    ```python
    >>> from owid.catalog import fetch
    >>> tb = fetch("garden/un/2024-07-12/un_wpp/population")
    >>> tb_ind = fetch("garden/un/2024-07-12/un_wpp/population#population")
    >>> chart_tb = fetch("life-expectancy")  # Chart slug auto-detected
    ```
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal

from owid.catalog.api import Client
from owid.catalog.api.charts import ChartResult
from owid.catalog.api.indicators import IndicatorResult
from owid.catalog.api.models import ResponseSet
from owid.catalog.api.tables import TableResult
from owid.catalog.core.paths import CatalogPath

# Pattern for chart slugs: alphanumeric with dashes and underscores only
_CHART_SLUG_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")

if TYPE_CHECKING:
    from owid.catalog.core.charts import ChartTable
    from owid.catalog.tables import Table


def search(
    name: str | None = None,
    *,
    kind: Literal["table", "indicator", "chart"] = "table",
    limit: int = 10,
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channel: str | None = None,
    match: Literal["exact", "contains", "regex", "fuzzy"] = "fuzzy",
    fuzzy_threshold: int = 70,
    case: bool = False,
) -> ResponseSet[TableResult] | ResponseSet[IndicatorResult] | ResponseSet[ChartResult]:
    """Search for available data without downloading (for browsing/discovery).

    This function searches for data in the catalog and returns a ResponseSet of results
    without downloading the actual data. Use this to explore and find the exact path or
    slug, then use fetch() to download the data.

    Args:
        name: Name or pattern to search for (e.g., "population", "gdp", "life-expectancy").
            Required for indicators and charts. Optional for tables (can filter by other params).
        kind: What to search for (default: "table"):

            - "table": Search catalog tables (returns ResponseSet[TableResult])
            - "indicator": Search indicators/variables (returns ResponseSet[IndicatorResult])
            - "chart": Search published charts (returns ResponseSet[ChartResult])
        limit: Maximum number of results to return (default: 10)
        namespace: Filter by namespace (e.g., "un", "worldbank"). Only for tables.
        version: Filter by specific version (e.g., "2024-01-15"). Only for tables.
        dataset: Filter by dataset name. Only for tables.
        channel: Filter by channel (e.g., "garden", "grapher"). Only for tables.
        match: Matching mode (default: "fuzzy" for typo-tolerance) (only for tables):

            - "exact": Exact string match
            - "contains": Substring match
            - "regex": Regular expression
            - "fuzzy": Typo-tolerant similarity matching
        fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching (default: 70).  Only for tables.
        case: Case-sensitive search (default: False).  Only for tables.

    Returns:
        Search results. Results can be indexed, iterated, and provide access to metadata without downloading data.

    Example:
        ```python
        # Search for tables (fuzzy search by default)
        results = search("population")
        print(f"Found {len(results)} tables")
        print(results[0].path)  # Access table path without downloading data

        # Search for indicators
        results = search("gdp", kind="indicator")
        print(results[0].title)

        # Search for charts
        results = search("life expectancy", kind="chart")
        print(results[0].slug)

        # Exact match for tables
        results = search("population", match="exact")

        # Filter tables by namespace and version
        results = search("wdi", namespace="worldbank_wdi", version="2024-01-10")

        # Then fetch the data you need:
        tb = results[0].fetch()
        ```

    Warning:
        For indicators and charts, filtering parameters (namespace, version, dataset, channel)
        are ignored as they don't apply to those search types.
    """
    # Validate name is provided for indicators and charts
    if name is None and kind in ("indicator", "chart"):
        raise ValueError(f"'name' is required when searching for {kind}s.")

    # Route to appropriate search method based on kind
    client = Client()

    if kind == "table":
        # Search tables using TablesAPI
        return client.tables.search(
            table=name,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=channel,
            match=match,
            fuzzy_threshold=fuzzy_threshold,
            case=case,
        )
    elif kind == "indicator":
        # Search indicators using IndicatorsAPI
        return client.indicators.search(name, limit=limit)
    elif kind == "chart":
        # Search charts using ChartsAPI
        return client.charts.search(name, limit=limit)
    else:
        raise ValueError(f"Invalid kind='{kind}'. Must be 'table', 'indicator', or 'chart'.")


def fetch(path: str) -> "Table | ChartTable":
    """Fetch data directly by path (auto-detects tables, indicators, or charts).

    This function downloads the data associated with the given path. It auto-detects
    whether you're accessing a table, indicator, or chart based on the path format.

    Args:
        path: Path to the data resource:

            - Table: "channel/namespace/version/dataset/table"
            - Indicator: "channel/namespace/version/dataset/table#variable"
            - Chart: "life-expectancy" (chart slug without '/' or '#')

    Returns:
        Table (for tables or indicators) or CharTable (for charts)

    Raises:
        ValueError: If path format is invalid or resource not found

    Example:
        ```python
        # Fetch table
        tb = fetch("garden/un/2024-07-12/un_wpp/population")
        print(tb.shape)
        print(tb.metadata)

        # Fetch indicator as Table (single column)
        tb = fetch("garden/un/2024-07-12/un_wpp/population#population")
        print(tb.columns)

        # Fetch chart data (slug auto-detected)
        tb = fetch("life-expectancy")
        print(tb.metadata.title)

        # Fetch from grapher channel
        tb = fetch("grapher/demography/2025-10-22/life_expectancy/life_expectancy_at_birth")
        ```
    """
    # Create client (reuses singleton internally)
    client = Client()

    # Detect path type based on structure:
    # - Contains "/" → catalog path (table or indicator)
    # - Contains "#" → indicator path
    # - Matches slug pattern (alphanumeric + dashes/underscores) → chart slug

    if "/" in path or "#" in path:
        # Catalog path (table or indicator)
        try:
            catalog_path = CatalogPath.from_str(path)

            if catalog_path.variable is not None:
                # Indicator path (table path with #variable fragment)
                # Fetch using IndicatorsAPI which returns Table
                return client.indicators.fetch(path)
            else:
                # Regular table path
                return client.tables.fetch(path)

        except ValueError as e:
            # Re-raise with more context
            raise ValueError(
                f"Invalid catalog path: '{path}'. "
                f"Expected format: 'channel/namespace/version/dataset/table' "
                f"or 'channel/namespace/version/dataset/table#variable'. "
                f"Error: {e}"
            ) from e

    elif _CHART_SLUG_PATTERN.match(path):
        # Chart slug (alphanumeric with dashes/underscores)
        return client.charts.fetch(path)

    else:
        raise ValueError(
            f"Invalid path format: '{path}'. "
            f"Expected a catalog path (with '/'), indicator path (with '#'), "
            f"or chart slug (alphanumeric with dashes/underscores)."
        )
