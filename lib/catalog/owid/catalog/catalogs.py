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

See migration guide: https://docs.owid.io/catalog-api-migration
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING

import pandas as pd
import requests

# Re-export internal classes for backwards compatibility
from .api.tables import (
    _CatalogFrame as CatalogFrame,
)
from .api.tables import (
    _CatalogSeries as CatalogSeries,
)
from .api.tables import (
    _LocalCatalog as LocalCatalog,
)
from .api.tables import (
    _PackageUpdateRequired as PackageUpdateRequired,
)
from .api.tables import (
    _read_frame as read_frame,
)
from .api.tables import (
    _RemoteCatalog as RemoteCatalog,
)
from .api.tables import (
    _save_frame as save_frame,
)

# Re-export constants
from .datasets import CHANNEL, PREFERRED_FORMAT, SUPPORTED_FORMATS, FileFormat

if TYPE_CHECKING:
    from .tables import Table

# Public constants
OWID_CATALOG_VERSION = 3
OWID_CATALOG_URI = "https://catalog.ourworldindata.org/"
S3_OWID_URI = "s3://owid-catalog"
OWID_SEARCH_API = "https://search.owid.io/indicators"
INDEX_FORMATS: list[FileFormat] = ["feather"]

# Global cache
REMOTE_CATALOG: RemoteCatalog | None = None


def _warn_deprecated(func_name: str, alternative: str) -> None:
    """Issue deprecation warning for legacy functions."""
    warnings.warn(
        f"owid.catalog.{func_name}() is deprecated and will be removed in v1.0. "
        f"Use {alternative} instead. "
        f"See: https://docs.owid.io/catalog-api-migration",
        DeprecationWarning,
        stacklevel=3,
    )


def _load_remote_catalog(channels: Iterable[CHANNEL]) -> RemoteCatalog:
    """Internal helper to load remote catalog."""
    global REMOTE_CATALOG

    if REMOTE_CATALOG and not (set(channels) <= set(REMOTE_CATALOG.channels)):
        REMOTE_CATALOG = RemoteCatalog(channels=list(set(REMOTE_CATALOG.channels) | set(channels)))

    if not REMOTE_CATALOG:
        REMOTE_CATALOG = RemoteCatalog(channels=channels)

    return REMOTE_CATALOG


def find(
    table: str | None = None,
    namespace: str | None = None,
    version: str | None = None,
    dataset: str | None = None,
    channels: Iterable[CHANNEL] = ("garden",),
) -> CatalogFrame:
    """Search remote catalog (DEPRECATED).

    **DEPRECATED**: Use Client().tables.search() instead.

    Convenience function that searches the default Our World in Data remote
    catalog. Automatically initializes and caches the catalog connection.

    Args:
        table: Table name pattern to search for (substring match).
        namespace: Namespace to filter by (e.g., 'un', 'worldbank').
        version: Version string to filter by (e.g., '2024-01-15').
        dataset: Dataset name to filter by.
        channels: Data channels to search (default: garden only).

    Returns:
        CatalogFrame containing matching tables.

    Example:
        ```python
        # DEPRECATED - use Client API instead
        from owid.catalog import find
        results = find(table="population")

        # RECOMMENDED:
        from owid.catalog import Client
        client = Client()
        results = client.tables.search(table="population")
        ```
    """
    _warn_deprecated("find", "Client().tables.search()")
    catalog = _load_remote_catalog(channels=channels)
    return catalog.find(table=table, namespace=namespace, version=version, dataset=dataset)


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
    return find(*args, **kwargs).load()  # type: ignore


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
    catalog = _load_remote_catalog(channels=channels)
    return catalog.find_latest(table=table, namespace=namespace, dataset=dataset, version=version)


def find_by_indicator(query: str, limit: int = 10) -> CatalogFrame:
    """Search by indicator (DEPRECATED).

    **DEPRECATED**: Use Client().indicators.search() instead.

    Uses the OWID search API to find indicators matching a natural
    language query, then returns a CatalogFrame that can load the
    full tables containing those indicators.

    Args:
        query: Natural language search query (e.g., "solar power generation").
        limit: Maximum number of results to return (default: 10).

    Returns:
        CatalogFrame with columns: indicator_title, indicator, score, then standard
        catalog columns (table, dataset, version, namespace, channel, is_public,
        dimensions, path, format).

    Example:
        ```python
        # DEPRECATED:
        from owid.catalog import find_by_indicator
        results = find_by_indicator("solar power")

        # RECOMMENDED:
        from owid.catalog import Client
        client = Client()
        results = client.indicators.search("solar power")
        ```
    """
    _warn_deprecated("find_by_indicator", "Client().indicators.search()")

    # Keep original implementation
    resp = requests.get(
        OWID_SEARCH_API,
        params={"query": query, "limit": limit},
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])

    rows = []
    for r in results:
        catalog_path = r.get("catalog_path", "")
        path_part, _, indicator = catalog_path.partition("#")
        parts = path_part.split("/")

        if len(parts) >= 4:
            channel, namespace, version, dataset = parts[0], parts[1], parts[2], parts[3]
            table = parts[4] if len(parts) > 4 else dataset
        else:
            channel, namespace, version, dataset, table = pd.NA, pd.NA, pd.NA, pd.NA, pd.NA
            path_part = pd.NA
            indicator = indicator if indicator else pd.NA

        rows.append(
            {
                "indicator_title": r.get("title"),
                "indicator": indicator if indicator else pd.NA,
                "score": r.get("score"),
                "table": table,
                "dataset": dataset,
                "version": version,
                "namespace": namespace,
                "channel": channel,
                "is_public": True,
                "path": path_part,
                "format": "parquet",
            }
        )

    frame = CatalogFrame(rows)
    frame._base_uri = OWID_CATALOG_URI

    if not frame.empty:
        catalog = _load_remote_catalog(channels=["grapher"])
        frame = frame.merge(catalog.frame[["path", "dimensions"]], on="path", how="left")
        cols = [c for c in frame.columns if c != "dimensions"]
        cols.insert(cols.index("is_public") + 1, "dimensions")
        frame = CatalogFrame(frame[cols])
        frame._base_uri = OWID_CATALOG_URI

    return frame


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
    "find_by_indicator",
    # Constants
    "CHANNEL",
    "OWID_CATALOG_URI",
    "OWID_CATALOG_VERSION",
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
