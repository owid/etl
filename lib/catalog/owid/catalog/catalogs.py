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
    table = results[0].fetch()
    ```

"""

from __future__ import annotations

# Re-export catalog classes for backwards compatibility
from owid.catalog.api.legacy import (
    CatalogFrame,
    CatalogSeries,
    ETLCatalog,
    LocalCatalog,
    PackageUpdateRequired,
    read_frame,
    save_frame,
)

# Re-export constants from utils
from owid.catalog.api.utils import DEFAULT_CATALOG_URL, INDEX_FORMATS, S3_OWID_URI

# Re-export constants
from owid.catalog.core.datasets import CHANNEL, FileFormat

__all__ = [
    # Classes (backwards compatibility)
    "LocalCatalog",
    "ETLCatalog",
    "CatalogFrame",
    "CatalogSeries",
    "PackageUpdateRequired",
    # Constants
    "CHANNEL",
    "DEFAULT_CATALOG_URL",
    "S3_OWID_URI",
    "INDEX_FORMATS",
    "FileFormat",
    # Utilities
    "read_frame",
    "save_frame",
]
