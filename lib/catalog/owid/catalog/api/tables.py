#
#  owid.catalog.api.tables
#
#  Tables API for querying and loading tables from the OWID catalog.
#
from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, cast

from owid.catalog.api.models import ResultSet, TableResult
from owid.catalog.api.utils import OWID_CATALOG_URI, S3_OWID_URI, ETLCatalog
from owid.catalog.datasets import CHANNEL

if TYPE_CHECKING:
    from owid.catalog.api import Client
    from owid.catalog.tables import Table


class TablesAPI:
    """API for querying and loading tables from the OWID catalog.

    Provides methods to search for tables by various criteria and
    load table data directly from the catalog.

    Example:
        ```python
        from owid.catalog import Client

        client = Client()

        # Search for tables
        results = client.tables.search(table="population", namespace="un")

        # Load the first result
        table = results[0].data

        # Get table data directly by path
        table = client.tables.get_data("garden/un/2024/population/population")

        # Or fetch metadata first
        table_result = client.tables.fetch("garden/un/2024/population/population")
        print(f"Dataset: {table_result.dataset}, Version: {table_result.version}")
        table = table_result.data  # Lazy-load when needed
        ```
    """

    CATALOG_URI = OWID_CATALOG_URI
    S3_URI = S3_OWID_URI

    def __init__(self, client: Client) -> None:
        self._client = client
        self._catalog: ETLCatalog | None = None
        self._channels: set[str] = {"garden"}

    def _get_catalog(self, channels: Iterable[str] = ("garden",)) -> ETLCatalog:
        """Get or create the remote catalog, adding channels as needed."""
        # Use ETLCatalog from utils

        # Add new channels if needed
        new_channels = set(channels) - self._channels
        if new_channels or self._catalog is None:
            self._channels = self._channels | set(channels)
            # Cast to the expected channel type
            channel_list: list = list(self._channels)  # type: ignore
            self._catalog = ETLCatalog(channels=channel_list)

        return self._catalog

    def _build_query(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
        channels: Iterable[str] = ("garden",),
    ) -> str:
        """Build a descriptive query string from search parameters.

        Args:
            table: Table name pattern
            namespace: Namespace filter
            version: Version filter
            dataset: Dataset name pattern
            channel: Channel filter
            channels: List of channels to search

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
        elif len(list(channels)) > 1 or list(channels)[0] != "garden":
            # Only show channels if non-default
            query_parts.append(f"channels={list(channels)!r}")

        return " ".join(query_parts) if query_parts else "all tables"

    def search(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
        channels: Iterable[str] = ("garden",),
        case: bool = False,
        regex: bool = True,
        fuzzy: bool = False,
        threshold: int = 70,
    ) -> ResultSet[TableResult]:
        """Search the catalog for tables matching criteria.

        Args:
            table: Table name pattern to search for
            namespace: Filter by namespace (exact match)
            version: Filter by version (exact match)
            dataset: Dataset name pattern to search for
            channel: Filter by channel (exact match)
            channels: List of channels to search (default: garden only)
            case: Case-sensitive search (default: False)
            regex: Enable regex patterns in table/dataset (default: True)
            fuzzy: Use fuzzy string matching (default: False)
            threshold: Minimum fuzzy match score 0-100 (default: 70)

        Returns:
            ResultSet containing matching TableResult objects.
            If fuzzy=True, results are sorted by relevance score.

        Examples:
            ```python
            # Exact match
            results = client.tables.search(table="population", regex=False)

            # Regex search (default)
            results = client.tables.search(table="population.*density")

            # Fuzzy search sorted by relevance
            results = client.tables.search(table="populaton", fuzzy=True)

            # Case-sensitive fuzzy search
            results = client.tables.search(table="GDP", fuzzy=True, case=True)

            # Filter by namespace and version
            results = client.tables.search(
                table="gdp",
                namespace="worldbank",
                version="2024-01-15"
            )

            # Search multiple channels
            results = client.tables.search(
                table="co2",
                channels=["garden", "meadow"]
            )

            # Load a specific result
            table = results[0].data
            ```
        """
        catalog = self._get_catalog(channels)

        # Use catalog.find() which now supports fuzzy matching
        matches = catalog.find(
            table=table,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=cast(CHANNEL, channel) if channel else None,
            case=case,
            regex=regex,
            fuzzy=fuzzy,
            threshold=threshold,
        )

        # Convert to TableResult objects
        results = []
        for _, row in matches.iterrows():
            dimensions = row.get("dimensions", [])
            if isinstance(dimensions, str):
                import json

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
            channels=channels,
        )

        return ResultSet(
            results=results,
            query=query,
            total=len(results),
        )

    def fetch(self, path: str, *, load_data: bool = False) -> TableResult:
        """Fetch table metadata by catalog path (without loading data).

        Returns metadata about the table. Access .data property on the result to
        lazy-load the table data.

        Args:
            path: Full catalog path (e.g., "garden/un/2024/population/population").
            load_data: If True, preload table data immediately.
                       If False (default), data is loaded lazily when accessed via .data property.

        Returns:
            TableResult with metadata. Access .data to get the table.

        Raises:
            ValueError: If table not found.

        Example:
            ```python
            # Get metadata without loading data
            result = client.tables.fetch("garden/un/2024/population/population")
            print(f"Dataset: {result.dataset}, Version: {result.version}")

            # Load data when needed
            table = result.data

            # Or preload data immediately
            result = client.tables.fetch(path, load_data=True)
            table = result.data  # Already loaded
            ```
        """
        # Parse path: channel/namespace/version/dataset/table
        parts = path.split("/")
        if len(parts) < 5:
            raise ValueError(f"Invalid path format: {path}. Expected format: channel/namespace/version/dataset/table")

        channel, namespace, version, dataset = parts[0:4]
        table = parts[4]

        # Search to get full metadata from catalog
        results = self.search(
            table=table,
            namespace=namespace,
            version=version,
            dataset=dataset,
            channel=channel,
            channels=[channel],  # Ensure the catalog loads this channel
        )

        if len(results) == 0:
            raise ValueError(f"Table not found: {path}")

        # Get first match (should be exact)
        result = results[0]

        # Preload data if requested
        if load_data:
            _ = result.data  # Access property to trigger loading

        return result

    def get_data(self, path: str) -> "Table":
        """Fetch table data directly.

        Convenience method equivalent to fetch(path).data

        Args:
            path: Full catalog path (e.g., "garden/un/2024/population/population").

        Returns:
            Table (pandas DataFrame with metadata).

        Raises:
            ValueError: If table not found.

        Example:
            ```python
            table = client.tables.get_data("garden/un/2024/population/population")
            print(table.head())
            print(table.metadata.title)
            ```
        """
        return self.fetch(path).data
