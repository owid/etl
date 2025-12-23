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

# Re-export catalog classes for backwards compatibility
from owid.catalog.api.catalogs import (
    CatalogFrame,
    CatalogSeries,
    ETLCatalog,
    LocalCatalog,
    PackageUpdateRequired,
    read_frame,
    save_frame,
)

# Re-export constants from utils
from owid.catalog.api.utils import INDEX_FORMATS, OWID_CATALOG_URI, S3_OWID_URI

# Re-export constants
from owid.catalog.datasets import CHANNEL, PREFERRED_FORMAT, SUPPORTED_FORMATS, FileFormat

if TYPE_CHECKING:
    from owid.catalog.api import Client

# Global cache for backwards compatibility
REMOTE_CATALOG: ETLCatalog | None = None
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
        from owid.catalog.api import Client

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

    # Maintain backwards compatibility by populating REMOTE_CATALOG
    global REMOTE_CATALOG
    if REMOTE_CATALOG is None:
        REMOTE_CATALOG = ETLCatalog(channels=channels)
    else:
        # Add new channels if needed
        current_channels = set(REMOTE_CATALOG.channels)
        new_channels = set(channels) - current_channels
        if new_channels:
            REMOTE_CATALOG = ETLCatalog(channels=tuple(current_channels | set(channels)))

    # Convert old parameters to new match parameter for backwards compatibility
    if fuzzy:
        match = "fuzzy"
    elif regex:
        match = "regex"
    else:
        match = "contains"

    # Use Client API internally with new parameters
    client = _get_client()
    results = client.tables.search(
        table=table,
        namespace=namespace,
        version=version,
        dataset=dataset,
        channels=channels,
        case=case,
        match=match,
        fuzzy_threshold=threshold,
    )

    # Convert ResultSet to CatalogFrame for backwards compatibility
    return results.to_catalog_frame()


__all__ = [
    # Classes (backwards compatibility)
    "LocalCatalog",
    "ETLCatalog",
    "CatalogFrame",
    "CatalogSeries",
    "PackageUpdateRequired",
    # Functions (deprecated)
    "find",
    # Constants
    "CHANNEL",
    "OWID_CATALOG_URI",
    "S3_OWID_URI",
    "INDEX_FORMATS",
    "PREFERRED_FORMAT",
    "SUPPORTED_FORMATS",
    "FileFormat",
    # Utilities
    "read_frame",
    "save_frame",
]
