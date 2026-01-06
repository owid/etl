#
#  owid.catalog.api.tables
#
#  Tables API for querying and loading tables from the OWID catalog.
#
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal, cast

from owid.catalog.api.catalogs import ETLCatalog
from owid.catalog.api.models import ResponseSet, TableResult, _load_table
from owid.catalog.api.utils import OWID_CATALOG_URI, S3_OWID_URI
from owid.catalog.core import CatalogPath
from owid.catalog.core.paths import VALID_CHANNELS
from owid.catalog.datasets import CHANNEL
from owid.catalog.tables import Table

if TYPE_CHECKING:
    from owid.catalog.api import Client


class TablesAPI:
    """API for querying and loading tables from the OWID catalog.

    Provides methods to search for tables by various criteria and
    load table data from the catalog.

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Search for tables
        results = client.tables.search(table="population", namespace="un")

        # Load the first result
        table = results[0].data

        # Fetch table directly by path
        tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
        print(tb.head())
        ```
    """

    CATALOG_URI = OWID_CATALOG_URI
    S3_URI = S3_OWID_URI

    def __init__(self, client: Client) -> None:
        self._client = client
        self._catalog: ETLCatalog | None = None

    def _get_catalog(self, timeout: int | None = None) -> ETLCatalog:
        """Get or create the remote catalog with all channels loaded.

        Args:
            timeout: HTTP request timeout in seconds. Defaults to client timeout.
                     Only used during initial catalog loading.
        """
        if self._catalog is None:
            # Load all available channels
            self._catalog = ETLCatalog(
                channels=cast(Iterable[CHANNEL], VALID_CHANNELS), timeout=timeout or self._client.timeout
            )

        return self._catalog

    def _build_query(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
    ) -> str:
        """Build a descriptive query string from search parameters.

        Args:
            table: Table name pattern
            namespace: Namespace filter
            version: Version filter
            dataset: Dataset name pattern
            channel: Channel filter (defaults to 'garden' if not specified)

        Returns:
            Human-readable query string describing the search parameters.
        """
        query_parts = []
        if table:
            query_parts.append(f"table={table!r}")
        if namespace:
            query_parts.append(f"namespace={namespace!r}")
        if version:
            query_parts.append(f"version={version!r}")
        if dataset:
            query_parts.append(f"dataset={dataset!r}")
        if channel:
            query_parts.append(f"channel={channel!r}")
        else:
            # Default to garden channel
            query_parts.append("channel='garden'")

        return " ".join(query_parts) if query_parts else "all tables"

    def search(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
        case: bool = False,
        match: Literal["exact", "contains", "regex", "fuzzy"] = "exact",
        fuzzy_threshold: int = 70,
        timeout: int | None = None,
    ) -> ResponseSet[TableResult]:
        """Search the catalog for tables matching criteria.

        Args:
            table: Table name pattern to search for
            namespace: Filter by namespace (exact match)
            version: Filter by version (exact match)
            dataset: Dataset name pattern to search for
            channel: Filter by channel (exact match). Defaults to 'garden' if not specified.
            case: Case-sensitive search (default: False)
            match: How to match table/dataset names (default: "exact"):
                - "exact": Exact string match
                - "contains": Substring match
                - "regex": Regular expression pattern
                - "fuzzy": Typo-tolerant similarity matching
            fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching.
                Only used when match="fuzzy". (default: 70)
            timeout: HTTP request timeout in seconds for catalog loading. Defaults to client timeout.

        Returns:
            ResponseSet containing matching TableResult objects. If match="fuzzy", results are sorted by relevance score.

        Example:
            ```python
            # Exact match (default) - searches garden channel by default
            results = client.tables.search(table="population")

            # Substring match
            results = client.tables.search(table="pop", match="contains")

            # Regex search
            results = client.tables.search(table="population.*density", match="regex")

            # Fuzzy search sorted by relevance
            results = client.tables.search(table="populaton", match="fuzzy")

            # Case-sensitive fuzzy search with custom threshold
            results = client.tables.search(table="GDP", match="fuzzy", case=True, fuzzy_threshold=85)

            # Filter by namespace and version
            results = client.tables.search(
                table="wdi",
                namespace="worldbank_wdi",
                version="2025-09-08",
            )

            # Search in a specific channel
            results = client.tables.search(
                table="wdi",
                namespace="worldbank_wdi",
                version="2025-09-08",
                channel="meadow",
            )

            # Load a specific result
            tb = results[0].data
            ```
        """
        # Default to garden channel if not specified
        if channel is None:
            channel = "garden"

        catalog = self._get_catalog(timeout=timeout)

        # Use catalog.find() with the new match parameter
        matches = catalog.find(
            table=table,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=cast(CHANNEL, channel) if channel else None,
            case=case,
            match=match,
            fuzzy_threshold=fuzzy_threshold,
        )

        # Convert to TableResult objects
        results = []
        for _, row in matches.iterrows():
            dimensions = row.get("dimensions", [])
            if isinstance(dimensions, str):
                dimensions = json.loads(dimensions)

            # Handle formats - could be list, numpy array, or None
            formats_raw = row.get("formats", None)
            if formats_raw is None or (hasattr(formats_raw, "__len__") and len(formats_raw) == 0):
                # Fallback to single format field
                format_single = row.get("format", None)
                formats = [format_single] if format_single else []
            else:
                formats = list(formats_raw) if hasattr(formats_raw, "__iter__") else []

            results.append(
                TableResult(
                    table=row["table"],
                    dataset=row["dataset"],
                    version=str(row["version"]),
                    namespace=row["namespace"],
                    channel=row["channel"],
                    path=row["path"],
                    is_public=row.get("is_public", True),
                    dimensions=list(dimensions) if dimensions is not None else [],
                    formats=formats,
                )
            )

        # Build descriptive query from search parameters
        query = self._build_query(
            table=table,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=channel,
        )

        return ResponseSet(
            results=results,
            query=query,
            total_count=len(results),
        )

    def fetch(self, path: str, *, load_data: bool = True, timeout: int | None = None) -> Table:
        """Fetch a table by catalog path.

        Loads the table directly from the catalog.

        Args:
            path: Full catalog path (e.g., "garden/un/2024-07-12/un_wpp/population").
            load_data: If True (default), load full table data.
                       If False, load only table structure (columns and metadata) without rows.
            timeout: HTTP request timeout in seconds (currently unused, reserved for future).

        Returns:
            Table with data and metadata (or just metadata if load_data=False).

        Raises:
            ValueError: If path format is invalid.
            KeyError: If table not found at path.

        Example:
            ```python
            # Load table with data
            tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
            print(tb.head())

            # Load only metadata (no data rows)
            tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population", load_data=False)
            print(tb.columns)
            ```
        """
        # Validate path format
        catalog_path = CatalogPath.from_str(path)

        if not catalog_path.table:
            raise ValueError(f"Invalid path format: {path}. Expected format: channel/namespace/version/dataset/table")

        return _load_table(path, load_data=load_data)
