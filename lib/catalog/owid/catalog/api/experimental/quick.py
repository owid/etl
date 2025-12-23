"""Quick access functions for common data retrieval patterns.

This module provides convenience functions for quickly accessing OWID catalog data
without the ceremony of creating a client and navigating search results.

Functions:
    quick() - Smart search with fuzzy matching for tables, indicators, or charts
    get() - Direct access by path (auto-detects tables, indicators, or charts)

Examples:
    >>> # Quick fuzzy search - returns latest version's data
    >>> from owid.catalog.api.experimental import quick
    >>> tb = quick("population")  # Finds and loads latest table

    >>> # Search for indicators (returns single-column Table)
    >>> tb_ind = quick("population", kind="indicator")

    >>> # Direct path access (auto-detects type using CatalogPath)
    >>> from owid.catalog.api.experimental import get
    >>> tb = get("garden/un/2024-07-12/un_wpp/population")
    >>> tb_ind = get("garden/un/2024-07-12/un_wpp/population#population")
    >>> df_chart = get("chart:life-expectancy")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import pandas as pd

from owid.catalog.api import Client
from owid.catalog.core.paths import CatalogPath

if TYPE_CHECKING:
    from owid.catalog.tables import Table


def quick(
    name: str,
    *,
    kind: Literal["table", "indicator", "chart"] = "table",
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channel: str | None = None,
    latest: bool = True,
    match: Literal["exact", "contains", "regex", "fuzzy"] = "fuzzy",
    fuzzy_threshold: int = 70,
    case: bool = False,
) -> "Table" | pd.DataFrame:
    """Quick search and download with sensible defaults.

    TODO: quick("population", kind="chart")

    This is a convenience function that wraps the appropriate API search method
    with smart defaults for common use cases:

    - Uses fuzzy matching by default (typo-tolerant)
    - Automatically returns the latest version if latest=True
    - Loads data immediately (no lazy loading)

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
        latest: If True, return latest version automatically (default: True)
        match: Matching mode (default: "fuzzy" for typo-tolerance)
            - "exact": Exact string match
            - "contains": Substring match
            - "regex": Regular expression
            - "fuzzy": Typo-tolerant similarity matching
        fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching (default: 70)
        case: Case-sensitive search (default: False)

    Returns:
        - Table object if kind="table"
        - Table object if kind="indicator" (single-column with metadata)
        - DataFrame if kind="chart"

    Raises:
        ValueError: If no results found or multiple results found with latest=False

    Example:
        ```python
        >>> # Simplest case - fuzzy search for table
        >>> tb = quick("population")

        >>> # Search for indicator (variable)
        >>> tb_ind = quick("population", kind="indicator")

        >>> # Search for chart
        >>> df = quick("life-expectancy", kind="chart")

        >>> # With namespace filter
        >>> quick("wdi", namespace="worldbank_wdi")

        >>> # Exact match (no fuzzy tolerance)
        >>> tb = quick("population", match="exact")

        >>> # Get specific version (not latest)
        >>> tb = quick("population", version="2024-12-01", latest=False)

        >>> # Search in specific channel
        >>> tb = quick("co2", channel="grapher")
        ```

    Future improvements:
        - Better communicate the ID/path of the returned resource
    """
    # Route to appropriate helper function based on kind
    if kind == "table":
        return _quick_table(
            name=name,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=channel,
            latest=latest,
            match=match,
            fuzzy_threshold=fuzzy_threshold,
            case=case,
        )
    elif kind == "indicator":
        return _quick_indicator(name=name, latest=latest)
    elif kind == "chart":
        return _quick_chart(name=name)
    else:
        raise ValueError(f"Invalid kind='{kind}'. Must be 'table', 'indicator', or 'chart'.")


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
        - Table object if path points to a table
        - Table object if path contains # fragment (single-column indicator)
        - DataFrame if path starts with "chart:" prefix

    Raises:
        ValueError: If path is invalid, uses unexpected prefix, or resource not found

    Example:
        ```python
        # Get table
        tb = get("garden/un/2024-07-12/un_wpp/population")

        # Get indicator as single-column Table
        tb = get("garden/un/2024-07-12/un_wpp/population#population")

        # Get chart data
        chart_data = get("chart:life-expectancy")

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


def _quick_table(
    name: str,
    *,
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channel: str | None = None,
    latest: bool = True,
    match: Literal["exact", "contains", "regex", "fuzzy"] = "fuzzy",
    fuzzy_threshold: int = 70,
    case: bool = False,
) -> "Table":
    """Helper function to search and retrieve table data."""
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
        error_msg = f"No tables found matching name='{name}'"
        if namespace:
            error_msg += f", namespace='{namespace}'"
        if version:
            error_msg += f", version='{version}'"
        raise ValueError(error_msg)

    # Get latest version if requested
    if latest:
        result = results.latest(by="version")
    else:
        # Without latest=True, require exactly one result
        if len(results) > 1:
            raise ValueError(
                f"Multiple tables found ({len(results)} results). "
                f"Either set latest=True to get the most recent version, "
                f"or add more filters (namespace, version, dataset) to narrow results."
            )
        result = results[0]

    # Load and return table data
    return result.data


def _quick_indicator(name: str, *, latest: bool = True) -> "Table":
    """Helper function to search and retrieve indicator data as a Table."""
    client = Client()

    # Search indicators
    results = client.indicators.search(name)

    # Handle no results
    if len(results) == 0:
        raise ValueError(f"No indicators found matching name='{name}'")

    # Get best match (indicators are sorted by relevance)
    if latest:
        # For indicators, get first result (best match by relevance)
        result = results[0]
    else:
        # Without latest=True, require exactly one result
        if len(results) > 1:
            raise ValueError(
                f"Multiple indicators found ({len(results)} results). "
                f"Set latest=True to get the best match by relevance."
            )
        result = results[0]

    # Load variable data and convert to Table
    variable = result.data
    return variable.to_frame()


def _quick_chart(name: str) -> pd.DataFrame:
    """Helper function to search and retrieve chart data."""
    client = Client()

    # Search charts
    results = client.charts.search(name)

    # Handle no results
    if len(results) == 0:
        raise ValueError(f"No charts found matching name='{name}'")

    # Get first match
    result = results[0]

    # Load and return chart data as DataFrame
    return client.charts.get_data(result.slug)
