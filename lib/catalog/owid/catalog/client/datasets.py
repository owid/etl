#
#  owid.catalog.client.datasets
#
#  Datasets API for querying and loading from the OWID catalog.
#
from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt

from .models import DatasetResult, ResultSet

if TYPE_CHECKING:
    from ..catalogs import RemoteCatalog
    from ..tables import Table
    from . import Client


# Default format preference
PREFERRED_FORMAT = "feather"
SUPPORTED_FORMATS = ["feather", "parquet", "csv"]


class DatasetsAPI:
    """API for querying and loading datasets from the OWID catalog.

    Provides methods to search for datasets by various criteria and
    load tables directly from the catalog.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Search for datasets
        results = client.datasets.search(table="population", namespace="un")

        # Load the first result
        table = results[0].load()

        # Fetch metadata by path (without loading data)
        dataset = client.datasets.fetch("garden/un/2024/population/population")
        print(f"Dataset: {dataset.dataset}, Version: {dataset.version}")
        table = dataset.load()  # Load when needed

        # Direct path access (loads data immediately)
        table = client.datasets["garden/un/2024-07-11/population/population"]
        ```
    """

    CATALOG_URI = "https://catalog.ourworldindata.org/"
    S3_URI = "s3://owid-catalog"

    def __init__(self, client: Client) -> None:
        self._client = client
        self._catalog: RemoteCatalog | None = None
        self._channels: set[str] = {"garden"}

    def _get_catalog(self, channels: Iterable[str] = ("garden",)) -> RemoteCatalog:
        """Get or create the remote catalog, adding channels as needed."""
        from ..catalogs import RemoteCatalog as RC

        # Add new channels if needed
        new_channels = set(channels) - self._channels
        if new_channels or self._catalog is None:
            self._channels = self._channels | set(channels)
            # Cast to the expected channel type
            channel_list: list = list(self._channels)  # type: ignore
            self._catalog = RC(channels=channel_list)

        return self._catalog

    def search(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
        channels: Iterable[str] = ("garden",),
    ) -> ResultSet[DatasetResult]:
        """Search the catalog for datasets matching criteria.

        Args:
            table: Table name pattern (substring match).
            namespace: Data provider namespace (e.g., "un", "worldbank").
            version: Version string (e.g., "2024-01-15").
            dataset: Dataset name.
            channel: Single channel to search.
            channels: List of channels to search (default: garden only).

        Returns:
            SearchResults containing DatasetResult objects.

        Example:
            ```python
            # Search by table name
            results = client.datasets.search(table="population")

            # Filter by namespace and version
            results = client.datasets.search(
                table="gdp",
                namespace="worldbank",
                version="2024-01-15"
            )

            # Search multiple channels
            results = client.datasets.search(
                table="co2",
                channels=["garden", "meadow"]
            )

            # Load a specific result
            table = results[0].load()
            ```
        """
        catalog = self._get_catalog(channels)
        frame = catalog.frame

        # Build filter criteria
        criteria: npt.ArrayLike = np.ones(len(frame), dtype=bool)

        if table:
            criteria &= frame.table.str.contains(table)
        if namespace:
            criteria &= frame.namespace == namespace
        if version:
            criteria &= frame.version == version
        if dataset:
            criteria &= frame.dataset == dataset
        if channel:
            criteria &= frame.channel == channel

        matches = frame[criteria]

        # Convert to DatasetResult objects
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
                DatasetResult(
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

        return ResultSet(
            results=results,
            query=table or "",
            total=len(results),
        )

    def fetch(self, path: str) -> DatasetResult:
        """Fetch dataset metadata by catalog path (without loading data).

        Returns metadata about the dataset. Use .load() on the result to
        actually load the table data.

        Args:
            path: Full catalog path (e.g., "garden/un/2024/population/population").

        Returns:
            DatasetResult with metadata. Call .load() to get the table.

        Raises:
            ValueError: If dataset not found.

        Example:
            ```python
            # Get metadata without loading data
            result = client.datasets.fetch("garden/un/2024/population/population")
            print(f"Dataset: {result.dataset}, Version: {result.version}")

            # Load data when needed
            table = result.load()
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
        )

        if len(results) == 0:
            raise ValueError(f"Dataset not found: {path}")

        # Return first match (should be exact)
        return results[0]

    def __getitem__(self, path: str) -> "Table":
        """Load a table by its catalog path.

        Args:
            path: Full catalog path (e.g., "garden/un/2024/population/population").

        Returns:
            The loaded Table object.

        Raises:
            KeyError: If no table found at the path.

        Example:
            ```python
            table = client.datasets["garden/un/2024-07-11/population/population"]
            ```
        """
        return self._load_table(path)

    @staticmethod
    def _load_table(
        path: str,
        formats: list[str] | None = None,
        is_public: bool = True,
    ) -> "Table":
        """Load a table from the catalog by path.

        Internal method used by DatasetResult.load() and __getitem__.
        """
        from ..tables import Table

        base_uri = DatasetsAPI.CATALOG_URI
        uri = "/".join([base_uri.rstrip("/"), path])

        # Determine format preference
        if formats:
            formats_to_try = formats
        else:
            formats_to_try = SUPPORTED_FORMATS

        # Prefer feather if available
        if PREFERRED_FORMAT in formats_to_try:
            formats_to_try = [PREFERRED_FORMAT] + [f for f in formats_to_try if f != PREFERRED_FORMAT]

        for fmt in formats_to_try:
            try:
                table_uri = f"{uri}.{fmt}"

                # Handle private files
                if not is_public:
                    table_uri = DatasetsAPI._download_private_file(table_uri)

                return Table.read(table_uri)
            except Exception:
                continue

        raise KeyError(f"No matching table found at: {path}")

    @staticmethod
    def _download_private_file(uri: str) -> str:
        """Download a private file from S3 to a temp location."""
        from .. import s3_utils

        tmpdir = tempfile.mkdtemp()
        parsed = urlparse(uri)
        base, ext = os.path.splitext(parsed.path)

        s3_utils.download(
            DatasetsAPI.S3_URI + base + ".meta.json",
            tmpdir + "/data.meta.json",
        )
        s3_utils.download(
            DatasetsAPI.S3_URI + base + ext,
            tmpdir + "/data" + ext,
        )

        return tmpdir + "/data" + ext
