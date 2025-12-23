"""Quick access functions for data discovery and retrieval.

This module provides convenience functions for discovering and accessing OWID catalog data.
The API separates discovery (browsing) from download (retrieval) for better cost awareness.

Functions:
    show() - Display available data without downloading (discovery/browsing)
    get() - Direct access by path (downloads data)

Example:
    ```python
    >>> # Browse available data (no download)
    >>> from owid.catalog.api.experimental import show
    >>> show("population")  # Displays matching paths

    >>> # Download specific data by path
    >>> from owid.catalog.api.experimental import get
    >>> tb = get("garden/un/2024-07-12/un_wpp/population")
    >>> tb_ind = get("garden/un/2024-07-12/un_wpp/population#population")
    >>> df_chart = get("chart:life-expectancy")
    ```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pandas as pd

from owid.catalog.api import Client
from owid.catalog.core.paths import CatalogPath

if TYPE_CHECKING:
    from owid.catalog.tables import Table


def show(
    name: str,
    *,
    kind: Literal["table", "indicator", "chart"] = "table",
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channel: str | None = None,
    match: Literal["exact", "contains", "regex", "fuzzy"] = "fuzzy",
    fuzzy_threshold: int = 70,
    case: bool = False,
    limit: int = 10,
) -> list[str]:
    """Display available data without downloading (for browsing/discovery).

    This function shows what data is available in the catalog without downloading it.
    Use this to explore and find the exact path, then use get() to download the data.

    Args:
        name: Name or pattern to search for (e.g., "population", "gdp", "life-expectancy")
        kind: What to search for (default: "table"):

                - "table": Search catalog tables
                - "indicator": Search indicators (variables in tables)
                - "chart": Search published charts
        namespace: Filter by namespace (e.g., "un", "worldbank")
        version: Filter by specific version (e.g., "2024-01-15")
        dataset: Filter by dataset name
        channel: Filter by channel (e.g., "garden", "grapher")
        match: Matching mode (default: "fuzzy" for typo-tolerance)

                - "exact": Exact string match
                - "contains": Substring match
                - "regex": Regular expression
                - "fuzzy": Typo-tolerant similarity matching
        fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching (default: 70)
        case: Case-sensitive search (default: False)
        limit: Maximum number of results to show (default: 10)

    Returns:
        List of matching paths, sorted alphabetically

    Example:
        ```python
        # Browse tables (fuzzy search by default)
        show("population")

        # Browse indicators
        show("gdp", kind="indicator")

        # Browse charts
        show("life-expectancy", kind="chart")

        # Exact match
        show("population", match="exact")

        # With filters
        show("wdi", namespace="worldbank_wdi")

        # Then download what you need:
        from owid.catalog.api.experimental import get
        tb = get("garden/un/2024-07-12/un_wpp/population")
        ```

    Tip:
        Copy a path from the results and use get(path) to download the data.
    """
    # Route to appropriate helper function based on kind
    if kind == "table":
        res = _show_tables(
            name=name,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=channel,
            match=match,
            fuzzy_threshold=fuzzy_threshold,
            case=case,
            limit=limit,
        )
    elif kind == "indicator":
        res = _show_indicators(name=name, limit=limit)
    elif kind == "chart":
        res = _show_charts(name=name, limit=limit)
    else:
        raise ValueError(f"Invalid kind='{kind}'. Must be 'table', 'indicator', or 'chart'.")

    # TODO: sort res based on relevance, or alphabetically, etc. Maybe we need an argument for this.

    return res


def get(path: str) -> "Table" | pd.DataFrame:
    """Get data directly by path (auto-detects tables, indicators, or charts).

    This function auto-detects what you're trying to access based on the path format:

    - Regular catalog path → Table
    - Path with #fragment → Table (single-column indicator data)
    - Path starting with "chart:" → DataFrame (chart data)

    Args:
        path: Path to the data resource:

            - Table: "channel/namespace/version/dataset/table"
            - Indicator: "channel/namespace/version/dataset/table#variable"
            - Chart: "chart:slug" (e.g., "chart:life-expectancy")

    Returns:
        - Table object if path points to a table or indicator (single-column indicator)
        - DataFrame if path starts with "chart:" prefix

    Raises:
        ValueError: If path is invalid, uses unexpected prefix, or resource not found

    Example:
        ```python
        # Get table
        tb = get("garden/un/2024-07-12/un_wpp/population")

        # Get indicator as single-column Table
        tb_ind = get("garden/un/2024-07-12/un_wpp/population#population")

        # Get chart data
        df_chart = get("chart:life-expectancy")

        # Grapher channel table
        tb = get("grapher/demography/2025-10-22/life_expectancy/life_expectancy_at_birth")
        ```
    """
    # Create client (reuses singleton internally)
    client = Client()

    # Check for prefix (colon before first slash)
    if ":" in path and (path.index(":") < path.index("/") if "/" in path else True):
        prefix, rest = path.split(":", 1)

        # Validate prefix
        if prefix != "chart":
            raise ValueError(
                f"Invalid path prefix '{prefix}:'. "
                f"Only 'chart:' prefix is supported for referencing charts. "
                f"For catalog paths, omit the prefix."
            )

        # Chart slug with prefix
        return client.charts.get_data(rest)

    # Use CatalogPath to detect if this is an indicator (has #fragment)
    try:
        catalog_path = CatalogPath.from_str(path)

        if catalog_path.variable is not None:
            # Indicator path (table path with #variable fragment)
            # Get variable and convert to Table
            variable = client.indicators.get_data(path)
            return variable.to_frame()
        else:
            # Regular table path
            return client.tables.get_data(path)

    except ValueError as e:
        # Re-raise with more context
        raise e


def _show_tables(
    name: str,
    *,
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channel: str | None = None,
    match: Literal["exact", "contains", "regex", "fuzzy"] = "fuzzy",
    fuzzy_threshold: int = 70,
    case: bool = False,
    limit: int = 10,
) -> list[str]:
    """Helper function to display tables matching search criteria."""
    client = Client()

    # Search tables
    results = client.tables.search(
        table=name,
        namespace=namespace,
        version=version,
        dataset=dataset,
        channel=channel,
        case=case,
        match=match,
        fuzzy_threshold=fuzzy_threshold,
    )

    # Handle no results
    if len(results) == 0:
        print(f"No tables found matching '{name}'.")
        print("\nTry:")
        print("  - Broader search terms")
        print("  - match='contains' instead of 'fuzzy'")
        print("  - Check spelling")
        return []

    # Build full paths from results
    all_paths = [result.path for result in results]

    return all_paths


def _show_indicators(name: str, *, limit: int = 10) -> list[str]:
    """Helper function to display indicators matching search criteria."""
    client = Client()

    # Search indicators
    results = client.indicators.search(name, limit=limit)

    # Handle no results
    if len(results) == 0:
        print(f"No indicators found matching '{name}'.")
        print("\nTry:")
        print("  - Broader search terms")
        print("  - Check spelling")
        return []

    # Build full paths from results
    all_paths = [result.path for result in results if result.path]

    return all_paths


def _show_charts(name: str, *, limit: int = 10) -> list[str]:
    """Helper function to display charts matching search criteria."""
    client = Client()

    # Search charts
    results = client.charts.search(name)

    # Handle no results
    if len(results) == 0:
        print(f"No charts found matching '{name}'.")
        print("\nTry:")
        print("  - Broader search terms")
        print("  - Check spelling")
        return []

    # Build full paths from results
    all_paths = [f"chart:{result.slug}" for result in results]

    return all_paths
