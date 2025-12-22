#
#  catalogs.py
#  owid-catalog
#
#  DEPRECATED: Use owid.catalog.Client API instead
#
"""Legacy catalog interface (deprecated).

**DEPRECATED**: This module is deprecated. Use the Client API instead.

Migration example:
    ```python
    # OLD (deprecated):
    from owid.catalog import find
    results = find(table="population", namespace="un")
    table = results.iloc[0].load()

    # NEW (recommended):
    from owid.catalog import Client
    client = Client()
    results = client.tables.search(table="population", namespace="un")
    table = results[0].data
    ```

"""

from __future__ import annotations

import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING

from deprecated import deprecated

# Re-export classes and functions for backwards compatibility
from .api.utils import (
    INDEX_FORMATS,
    OWID_CATALOG_URI,
    S3_OWID_URI,
    CatalogFrame,
    CatalogSeries,
    LocalCatalog,
    PackageUpdateRequired,
    RemoteCatalog,
    read_frame,
    save_frame,
)

# Re-export constants
from .datasets import CHANNEL, PREFERRED_FORMAT, SUPPORTED_FORMATS, FileFormat

if TYPE_CHECKING:
    from .api import Client
    from .tables import Table

# Public constants
OWID_SEARCH_API = "https://search.owid.io/indicators"

# Global cache for backwards compatibility
REMOTE_CATALOG: RemoteCatalog | None = None
_CLIENT_INSTANCE: "Client | None" = None


def _warn_deprecated(func_name: str, alternative: str) -> None:
    """Issue deprecation warning for legacy functions."""
    warnings.warn(
        f"owid.catalog.{func_name}() is deprecated and will be removed in v1.0. "
        f"Use {alternative} instead. "
        f"See: https://docs.owid.io/catalog-api-migration",
        DeprecationWarning,
        stacklevel=3,
    )


def _get_client() -> "Client":
    """Get or create the global Client instance."""
    global _CLIENT_INSTANCE
    if _CLIENT_INSTANCE is None:
        from .api import Client

        _CLIENT_INSTANCE = Client()
    return _CLIENT_INSTANCE


@deprecated(
    version="0.4.0",
    reason="Use Client().tables.search() instead. See: https://docs.owid.io/catalog-api-migration",
)
def find(
    table: str | None = None,
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channels: Iterable[CHANNEL] = ("garden",),
    case: bool = False,
    regex: bool = True,
    fuzzy: bool = False,
    threshold: int = 70,
) -> CatalogFrame:
    """Search remote catalog (DEPRECATED).

    **DEPRECATED**: Use Client().tables.search() instead.

    Convenience function that searches the default Our World in Data remote
    catalog. Automatically initializes and caches the catalog connection.

    Args:
        table: Table name pattern to search for
        namespace: Filter by namespace (exact match)
        version: Filter by version (exact match)
        dataset: Dataset name pattern to search for
        channels: Data channels to search (default: garden only)
        case: Case-sensitive search (default: False)
        regex: Enable regex patterns in table/dataset (default: True)
        fuzzy: Use fuzzy string matching (default: False)
        threshold: Minimum fuzzy match score 0-100 (default: 70)

    Returns:
        CatalogFrame containing matching tables, sorted by relevance if fuzzy=True.

    Example:
        ```python
        # DEPRECATED - use Client API instead
        from owid.catalog import find
        results = find(table="population")
        results = find(table="populaton", fuzzy=True)  # Fuzzy search

        # RECOMMENDED:
        from owid.catalog import Client
        client = Client()
        results = client.tables.search(table="population")
        ```
    """
    _warn_deprecated("find", "Client().tables.search()")

    # Use Client API internally
    client = _get_client()
    results = client.tables.search(
        table=table,
        namespace=namespace,
        version=version,
        dataset=dataset,
        channels=channels,
        case=case,
        regex=regex,
        fuzzy=fuzzy,
        threshold=threshold,
    )

    # Convert ResultSet to CatalogFrame for backwards compatibility
    return results.to_catalog_frame()


@deprecated(
    version="0.4.0",
    reason="Use Client().tables.search()[0].data instead. See: https://docs.owid.io/catalog-api-migration",
)
def find_one(*args: str | None, **kwargs: str | None) -> Table:
    """Find and load single table (DEPRECATED).

    **DEPRECATED**: Use Client().tables.search()[0].data instead.

    Convenience function that combines find() and load() in one call.
    Requires exactly one matching table.

    Args:
        *args: Positional arguments passed to find().
        **kwargs: Keyword arguments passed to find().

    Returns:
        The loaded Table object.

    Raises:
        ValueError: If zero or multiple tables match the criteria.

    Example:
        ```python
        # DEPRECATED:
        from owid.catalog import find_one
        table = find_one(table="population", namespace="un")

        # RECOMMENDED:
        from owid.catalog import Client
        client = Client()
        results = client.tables.search(table="population", namespace="un")
        table = results[0].data
        ```
    """
    _warn_deprecated("find_one", "Client().tables.search()[0].data")

    # Use Client API internally
    client = _get_client()
    results = client.tables.search(*args, **kwargs)  # type: ignore

    if len(results) == 0:
        raise ValueError("no tables found")
    elif len(results) > 1:
        raise ValueError(
            f"only one table can be loaded at once (tables found: {', '.join([r.table for r in results])})"
        )

    return results[0].data


@deprecated(
    version="0.4.0",
    reason="Use Client().tables.search()[-1].data instead. See: https://docs.owid.io/catalog-api-migration",
)
def find_latest(
    table: str | None = None,
    namespace: str | None = None,
    dataset: str | None = None,
    channels: Iterable[CHANNEL] = ("garden",),
    version: str | None = None,
) -> Table:
    """Find latest version (DEPRECATED).

    **DEPRECATED**: Use Client().tables.search()[-1].data instead.

    Searches for tables matching the criteria and returns the one with the
    most recent version string (lexicographically sorted). Useful for always
    getting the most up-to-date data without specifying an exact version.

    Args:
        table: Table name pattern to search for (substring match).
        namespace: Namespace to filter by (e.g., 'un', 'worldbank').
        dataset: Dataset name to filter by.
        channels: Data channels to search (default: garden only).
        version: Optional specific version to load instead of latest.

    Returns:
        The loaded Table with the latest version.

    Raises:
        ValueError: If no tables match the criteria.

    Example:
        ```python
        # DEPRECATED:
        from owid.catalog import find_latest
        table = find_latest(table="population", namespace="un")

        # RECOMMENDED:
        from owid.catalog import Client
        client = Client()
        results = client.tables.search(table="population", namespace="un")
        table = results[-1].data  # Latest version
        ```
    """
    _warn_deprecated("find_latest", "Client().tables.search()[-1].data")

    # Use Client API internally
    client = _get_client()
    results = client.tables.search(
        table=table,
        namespace=namespace,
        version=version,
        dataset=dataset,
        channels=channels,
    )

    if len(results) == 0:
        raise ValueError("No matching table found")

    # Sort by version and get the latest
    sorted_results = sorted(results, key=lambda r: r.version)
    return sorted_results[-1].data


__all__ = [
    # Classes (backwards compatibility)
    "LocalCatalog",
    "RemoteCatalog",
    "CatalogFrame",
    "CatalogSeries",
    "PackageUpdateRequired",
    # Functions (deprecated)
    "find",
    "find_one",
    "find_latest",
    # Constants
    "CHANNEL",
    "OWID_CATALOG_URI",
    "S3_OWID_URI",
    "OWID_SEARCH_API",
    "INDEX_FORMATS",
    "PREFERRED_FORMAT",
    "SUPPORTED_FORMATS",
    "FileFormat",
    # Utilities
    "read_frame",
    "save_frame",
]
