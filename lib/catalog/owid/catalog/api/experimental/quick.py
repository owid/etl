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
    >>> df_chart = get("life-expectancy")  # Chart slug auto-detected
    ```
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal

import pandas as pd

from owid.catalog.api import Client
from owid.catalog.core.paths import CatalogPath

# Pattern for chart slugs: alphanumeric with dashes and underscores only
_CHART_SLUG_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")

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

    This function load the data associated to `path`. It auto-detects what you're trying to access (table, indicator or chart data) based on its format.

    Args:
        path: Path to the data resource:

            - Table: "channel/namespace/version/dataset/table"
            - Indicator: "channel/namespace/version/dataset/table#variable"
            - Chart: "life-expectancy" (e.g. no use of '/' or '#')

    Returns:
        - Table object if path points to a table or indicator
        - DataFrame if path is a chart slug

    Raises:
        ValueError: If path is invalid or resource not found

    Example:
        ```python
        # Get table
        tb = get("garden/un/2024-07-12/un_wpp/population")

        # Get indicator as single-column Table
        tb_ind = get("garden/un/2024-07-12/un_wpp/population#population")

        # Get chart data (slug auto-detected)
        df_chart = get("life-expectancy")

        # Grapher channel table
        tb = get("grapher/demography/2025-10-22/life_expectancy/life_expectancy_at_birth")
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
                # Get variable and convert to Table
                variable = client.indicators.get_data(path)
                return variable.to_frame()
            else:
                # Regular table path
                return client.tables.get_data(path)

        except ValueError as e:
            # Re-raise with more context
            raise e

    elif _CHART_SLUG_PATTERN.match(path):
        # Chart slug (alphanumeric with dashes/underscores)
        return client.charts.get_data(path)

    else:
        raise ValueError(
            f"Invalid path format: '{path}'. "
            f"Expected a catalog path (with '/'), indicator path (with '#'), "
            f"or chart slug (alphanumeric with dashes/underscores)."
        )


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

    # Build paths from results (just the slug, auto-detected by get())
    all_paths = [result.slug for result in results]

    return all_paths
