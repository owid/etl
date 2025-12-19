#
#  owid.catalog.api.tables
#
#  Tables API for querying and loading tables from the OWID catalog.
#
from __future__ import annotations

import heapq
import json
import os
import re
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt
import pandas as pd
import requests
import structlog

from .. import s3_utils
from ..datasets import CHANNEL, Dataset, FileFormat
from .models import ResultSet, TableResult

if TYPE_CHECKING:
    from ..tables import Table
    from . import Client

log = structlog.get_logger()

# Constants
OWID_CATALOG_VERSION = 3
OWID_CATALOG_URI = "https://catalog.ourworldindata.org/"
S3_OWID_URI = "s3://owid-catalog"
PREFERRED_FORMAT = "feather"
SUPPORTED_FORMATS = ["feather", "parquet", "csv"]


#
# Internal catalog implementation classes
# These are not part of the public API - use Client() instead
#


def _download_private_file(uri: str, tmpdir: str) -> str:
    """Download private files from S3 to temporary directory."""
    parsed = urlparse(uri)
    base, ext = os.path.splitext(parsed.path)
    s3_utils.download(
        S3_OWID_URI + base + ".meta.json",
        tmpdir + "/data.meta.json",
    )
    s3_utils.download(
        S3_OWID_URI + base + ext,
        tmpdir + "/data" + ext,
    )
    return tmpdir + "/data" + ext


def _read_frame(uri: str | Path) -> pd.DataFrame:
    """Read catalog index from various formats."""
    if isinstance(uri, Path):
        uri = str(uri)

    if uri.endswith(".feather"):
        return cast(pd.DataFrame, pd.read_feather(uri))
    elif uri.endswith(".parquet"):
        return cast(pd.DataFrame, pd.read_parquet(uri))
    elif uri.endswith(".csv"):
        return pd.read_csv(uri)

    raise ValueError(f"could not detect format of uri: {uri}")


def _save_frame(df: pd.DataFrame, path: str | Path) -> None:
    """Save catalog index in specified format."""
    path = str(path)
    if path.endswith(".feather"):
        df.to_feather(path)
    elif path.endswith(".parquet"):
        df.to_parquet(path)
    elif path.endswith(".csv"):
        df.to_csv(path)
    else:
        raise ValueError(f"could not detect what format to write to: {path}")


class CatalogMixin:
    """Abstract catalog interface (internal implementation class)."""

    channels: Iterable[CHANNEL]
    frame: "_CatalogFrame"
    uri: str

    def find(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: CHANNEL | None = None,
    ) -> "_CatalogFrame":
        """Search catalog for tables matching specified criteria."""
        criteria: npt.ArrayLike = np.ones(len(self.frame), dtype=bool)

        if table:
            criteria &= self.frame.table.str.contains(table)
        if namespace:
            criteria &= self.frame.namespace == namespace
        if version:
            criteria &= self.frame.version == version
        if dataset:
            criteria &= self.frame.dataset == dataset
        if channel:
            if channel not in self.channels:
                raise ValueError(
                    f"You need to add `{channel}` to channels in Catalog init (only `{self.channels}` are loaded now)"
                )
            criteria &= self.frame.channel == channel

        matches = self.frame[criteria]
        if "checksum" in matches.columns:
            matches = matches.drop(columns=["checksum"])

        return cast(_CatalogFrame, matches)

    def find_one(self, *args: str | None, **kwargs: str | None) -> Table:
        """Find and load a single table matching search criteria."""
        return self.find(*args, **kwargs).load()  # type: ignore

    def find_latest(
        self,
        *args: str | None,
        **kwargs: str | None,
    ) -> Table:
        """Find and load the latest version of a table."""
        frame = self.find(*args, **kwargs)  # type: ignore
        if frame.empty:
            raise ValueError("No matching table found")
        else:
            from ..tables import Table as T

            return cast(T, frame.sort_values("version").iloc[-1].load())

    def __getitem__(self, path: str) -> Table:
        from ..tables import Table as T

        uri = "/".join([self.uri.rstrip("/"), path])
        for _format in SUPPORTED_FORMATS:
            try:
                return T.read(f"{uri}.{_format}")
            except Exception:
                continue

        raise KeyError(f"no matching table found at: {uri}")


class _LocalCatalog(CatalogMixin):
    """Local filesystem catalog (internal implementation class)."""

    uri: str

    def __init__(self, path: str | Path, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = str(path)
        self.channels = channels
        if self._catalog_exists(channels):
            self.frame = _CatalogFrame(self._read_channels(channels))
            self.frame._base_uri = self.path.as_posix() + "/"
        else:
            self.reindex()

    @property
    def path(self) -> Path:
        return Path(self.uri)

    def _catalog_exists(self, channels: Iterable[CHANNEL]) -> bool:
        return all([self._catalog_channel_file(channel).exists() for channel in channels])

    def _catalog_channel_file(self, channel: CHANNEL, format: FileFormat = PREFERRED_FORMAT) -> Path:
        return self.path / f"catalog-{channel}.{format}"

    @property
    def _metadata_file(self) -> Path:
        return self.path / "catalog.meta.json"

    def _read_channels(self, channels: Iterable[CHANNEL]) -> pd.DataFrame:
        """Read selected channels from local path."""
        df = pd.concat([_read_frame(self._catalog_channel_file(channel)) for channel in channels])
        df.dimensions = df.dimensions.map(lambda s: json.loads(s) if isinstance(s, str) else s)
        return df

    def iter_datasets(self, channel: CHANNEL, include: str | None = None) -> Iterator[Dataset]:
        to_search = [self.path / channel]
        if not to_search[0].exists():
            return

        re_search = re.compile(include or "")

        while to_search:
            dir = heapq.heappop(to_search)
            if (dir / "index.json").exists() and re_search.search(str(dir)):
                yield Dataset(dir)
                continue

            for child in dir.iterdir():
                if child.is_dir():
                    heapq.heappush(to_search, child)

    def reindex(self, include: str | None = None) -> None:
        """Rebuild the catalog index by scanning the directory tree."""
        index = self._scan_for_datasets(include)

        if include:
            index = self._merge_index(self.frame, index)

        index._base_uri = self.path.as_posix() + "/"
        index.version = index.version.astype(str)
        index.dimensions = index.dimensions.map(lambda s: json.loads(s) if isinstance(s, str) else s)

        self._save_index(index)
        self.frame = index

    @staticmethod
    def _merge_index(frame: "_CatalogFrame", update: "_CatalogFrame") -> "_CatalogFrame":
        """Merge two indexes."""
        return _CatalogFrame(
            pd.concat(
                [update, frame.loc[~frame.path.isin(update.path)]],
                ignore_index=True,
            )
        )

    def _save_index(self, frame: "_CatalogFrame") -> None:
        """Save all channels to disk in separate catalog files."""
        from ..datasets import FileFormat as FF

        INDEX_FORMATS: list[FF] = ["feather"]  # type: ignore

        for channel in self.channels:
            channel_frame = frame.loc[frame.channel == channel].reset_index(drop=True)
            for format in INDEX_FORMATS:
                filename = self._catalog_channel_file(channel, format)
                _save_frame(channel_frame, filename)

        self._save_metadata({"format_version": OWID_CATALOG_VERSION})

    def _scan_for_datasets(self, include: str | None = None) -> "_CatalogFrame":
        """Scan datasets. You can filter by `include` to get better performance."""
        frames = []
        log.info("reindex.start", channels=self.channels, include=include)
        for channel in self.channels:
            channel_frames = []
            for ds in self.iter_datasets(channel, include=include):
                channel_frames.append(ds.index(self.path))
            frames += channel_frames
            log.info(
                "reindex",
                channel=channel,
                datasets=len(channel_frames),
                include=include,
            )

        df = pd.concat(frames, ignore_index=True)

        keys = ["table", "dataset", "version", "namespace", "channel", "is_public"]
        columns = keys + [c for c in df.columns if c not in keys]

        df.sort_values(keys, inplace=True)  # type: ignore
        df = df.loc[:, columns]

        return _CatalogFrame(df)

    def _save_metadata(self, contents: dict[str, Any]) -> None:
        with open(self._metadata_file, "w") as ostream:
            json.dump(contents, ostream, indent=2)


class _RemoteCatalog(CatalogMixin):
    """Remote HTTP catalog (internal implementation class)."""

    uri: str

    def __init__(self, uri: str = OWID_CATALOG_URI, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = uri
        self.channels = channels
        self.metadata = self._read_metadata(self.uri + "catalog.meta.json")
        if self.metadata["format_version"] > OWID_CATALOG_VERSION:
            raise _PackageUpdateRequired(
                f"library supports api version {OWID_CATALOG_VERSION}, "
                f"but the remote catalog has version {self.metadata['format_version']} "
                "-- please update"
            )

        self.frame = _CatalogFrame(self._read_channels(uri, channels))
        self.frame._base_uri = uri

    @property
    def datasets(self) -> pd.DataFrame:
        return self.frame[["namespace", "version", "dataset"]].drop_duplicates()

    @staticmethod
    def _read_metadata(uri: str) -> dict[str, Any]:
        """Read the metadata JSON blob for this repo."""
        resp = requests.get(uri)
        resp.raise_for_status()
        return cast(dict[str, Any], resp.json())

    @staticmethod
    def _read_channels(uri: str, channels: Iterable[CHANNEL]) -> pd.DataFrame:
        """Read selected channels from S3."""
        return pd.concat([_read_frame(uri + f"catalog-{channel}.{PREFERRED_FORMAT}") for channel in channels])


class _CatalogFrame(pd.DataFrame):
    """DataFrame of catalog search results (internal implementation class)."""

    _base_uri: str | None = None
    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return _CatalogFrame

    @property
    def _constructor_sliced(self) -> Any:
        def build(*args: Any, **kwargs: Any) -> Any:
            c = _CatalogSeries(*args, **kwargs)
            c._base_uri = self._base_uri
            return c

        return build

    def load(self) -> Table:
        if len(self) == 1:
            return self.iloc[0].load()  # type: ignore
        elif len(self) == 0:
            raise ValueError("no tables found")
        else:
            raise ValueError(f"only one table can be loaded at once (tables found: {', '.join(self.table.tolist())})")

    @staticmethod
    def create_empty() -> "_CatalogFrame":
        return _CatalogFrame(
            {
                "namespace": [],
                "version": [],
                "table": [],
                "dimensions": [],
                "path": [],
                "format": [],
            }
        )


class _CatalogSeries(pd.Series):
    """Single catalog search result row (internal implementation class)."""

    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return _CatalogSeries

    def load(self) -> Table:
        from ..tables import Table as T

        format = None
        if hasattr(self, "format"):
            format = self.format
        elif hasattr(self, "formats") and (self.formats is not None) and len(self.formats) > 0:
            format = PREFERRED_FORMAT if PREFERRED_FORMAT in self.formats else self.formats[0]

        if self.path and format and self._base_uri:
            with tempfile.TemporaryDirectory() as tmpdir:
                uri = self._base_uri + self.path + "." + format

                if not getattr(self, "is_public", True):
                    uri = _download_private_file(uri, tmpdir)

                return T.read(uri)

        raise ValueError("series is not a table spec")


class _PackageUpdateRequired(Exception):
    """Raised when catalog format version is newer than library version."""

    pass


class TablesAPI:
    """API for querying and loading tables from the OWID catalog.

    Provides methods to search for tables by various criteria and
    load table data directly from the catalog.

    Example:
        ```python
        from owid.catalog.client import Client

        client = Client()

        # Search for tables
        results = client.tables.search(table="population", namespace="un")

        # Load the first result
        table = results[0].data

        # Fetch metadata by path (without loading data)
        table_result = client.tables.fetch("garden/un/2024/population/population")
        print(f"Dataset: {table_result.dataset}, Version: {table_result.version}")
        table = table_result.data  # Lazy-load when needed

        # Direct path access (loads data immediately)
        table = client.tables["garden/un/2024-07-11/population/population"]
        ```
    """

    CATALOG_URI = "https://catalog.ourworldindata.org/"
    S3_URI = "s3://owid-catalog"

    def __init__(self, client: Client) -> None:
        self._client = client
        self._catalog: _RemoteCatalog | None = None
        self._channels: set[str] = {"garden"}

    def _get_catalog(self, channels: Iterable[str] = ("garden",)) -> _RemoteCatalog:
        """Get or create the remote catalog, adding channels as needed."""
        # Use internal _RemoteCatalog class directly

        # Add new channels if needed
        new_channels = set(channels) - self._channels
        if new_channels or self._catalog is None:
            self._channels = self._channels | set(channels)
            # Cast to the expected channel type
            channel_list: list = list(self._channels)  # type: ignore
            self._catalog = _RemoteCatalog(channels=channel_list)

        return self._catalog

    def search(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: str | None = None,
        channels: Iterable[str] = ("garden",),
    ) -> ResultSet[TableResult]:
        """Search the catalog for tables matching criteria.

        Args:
            table: Table name pattern (substring match).
            namespace: Data provider namespace (e.g., "un", "worldbank").
            version: Version string (e.g., "2024-01-15").
            dataset: Dataset name.
            channel: Single channel to search.
            channels: List of channels to search (default: garden only).

        Returns:
            SearchResults containing TableResult objects.

        Example:
            ```python
            # Search by table name
            results = client.tables.search(table="population")

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

        return ResultSet(
            results=results,
            query=table or "",
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
        )

        if len(results) == 0:
            raise ValueError(f"Table not found: {path}")

        # Get first match (should be exact)
        result = results[0]

        # Preload data if requested
        if load_data:
            _ = result.data  # Access property to trigger loading

        return result

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
            table = client.tables["garden/un/2024-07-11/population/population"]
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

        Internal method used by TableResult._load() and __getitem__.
        """
        from ..tables import Table

        base_uri = TablesAPI.CATALOG_URI
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
                    table_uri = TablesAPI._download_private_file(table_uri)

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
            TablesAPI.S3_URI + base + ".meta.json",
            tmpdir + "/data.meta.json",
        )
        s3_utils.download(
            TablesAPI.S3_URI + base + ext,
            tmpdir + "/data" + ext,
        )

        return tmpdir + "/data" + ext
