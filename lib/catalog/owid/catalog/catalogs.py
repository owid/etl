#
#  catalog.py
#  owid-catalog-py
#
from __future__ import annotations

import heapq
import json
import os
import re
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt
import pandas as pd
import requests
import structlog
from rapidfuzz import fuzz

from . import s3_utils
from .datasets import CHANNEL, PREFERRED_FORMAT, SUPPORTED_FORMATS, Dataset, FileFormat
from .tables import Table

log = structlog.get_logger()

# increment this on breaking changes to require clients to update
OWID_CATALOG_VERSION = 3

# location of the default remote catalog
OWID_CATALOG_URI = "https://catalog.ourworldindata.org/"

# S3 location for private files
S3_OWID_URI = "s3://owid-catalog"

# Semantic search API
OWID_SEARCH_API = "https://search.owid.io/indicators"

# global copy cached after first request
REMOTE_CATALOG: RemoteCatalog | None = None

# what formats should we for our index of available datasets?
INDEX_FORMATS: list[FileFormat] = ["feather"]


class CatalogMixin:
    """Abstract catalog interface for finding and loading data.

    Provides the core API for querying and retrieving data from catalogs.
    Used as a base class for both LocalCatalog and RemoteCatalog.

    Attributes:
        channels: Data channels to search (e.g., 'garden', 'meadow', 'grapher').
        frame: Internal DataFrame containing catalog index.
        uri: Base URI for the catalog location.
    """

    channels: Iterable[CHANNEL]
    frame: "CatalogFrame"
    uri: str

    def find(
        self,
        table: str | None = None,
        namespace: str | None = None,
        version: str | None = None,
        dataset: str | None = None,
        channel: CHANNEL | None = None,
        case: bool = False,
        regex: bool = True,
        fuzzy: bool = False,
        threshold: int = 70,
    ) -> "CatalogFrame":
        """Search catalog for tables matching specified criteria.

        Search supports pattern matching on table and dataset names (case-insensitive
        regex by default) and exact matching on other fields. Multiple criteria can
        be combined.

        Args:
            table: Table name pattern to search for (regex by default).
            namespace: Namespace to filter by (e.g., 'un', 'worldbank').
            version: Version string to filter by (e.g., '2024-01-15').
            dataset: Dataset name pattern to filter by (regex by default).
            channel: Data channel to search (e.g., 'garden', 'grapher').
            case: If True, search is case-sensitive. Default False.
            regex: If True, treat table/dataset as regex patterns. Default True.
                Ignored when fuzzy=True.
            fuzzy: If True, use fuzzy matching for table/dataset. Default False.
                When enabled, matches strings that are similar but not exact.
            threshold: Minimum fuzzy match score (0-100). Default 70.
                Only used when fuzzy=True. Higher values require closer matches.

        Returns:
            CatalogFrame containing matching tables.

        Raises:
            ValueError: If specified channel is not loaded in this catalog.

        Example:
            Search for GDP tables in World Bank namespace
            ```python
            from owid.catalog import find

            # Case-insensitive regex search (default)
            results = find(table="gdp.*capita", namespace="worldbank")

            # Case-sensitive literal search
            results = find(table="GDP", case=True, regex=False)

            # Fuzzy search - matches similar strings
            results = find(table="gdp per capita", fuzzy=True)
            results = find(dataset="wrld bank", fuzzy=True, threshold=60)
            ```
        """
        # Step 1: Apply exact match filters (namespace, version, channel)
        criteria: npt.NDArray[np.bool_] = np.ones(len(self.frame), dtype=bool)

        if namespace:
            criteria &= self.frame.namespace == namespace

        if version:
            criteria &= self.frame.version == version

        if channel:
            if channel not in self.channels:
                raise ValueError(
                    f"You need to add `{channel}` to channels in Catalog init (only `{self.channels}` are loaded now)"
                )
            criteria &= self.frame.channel == channel

        # Step 2: Apply text field filters (table and/or dataset)
        scores: npt.NDArray[np.float64] | None = None
        if table:
            table_scores = _match_score(self.frame.table, table, fuzzy, case, regex)
            criteria &= table_scores >= threshold
            scores = table_scores
        if dataset:
            dataset_scores = _match_score(self.frame.dataset, dataset, fuzzy, case, regex)
            criteria &= dataset_scores >= threshold
            # Average scores if both table and dataset are specified
            scores = dataset_scores if scores is None else (scores + dataset_scores) / 2

        # Step 3: Build result
        matches = self.frame[criteria]
        if "checksum" in matches.columns:
            matches = matches.drop(columns=["checksum"])

        # Sort by score (descending) when fuzzy matching is used
        if fuzzy and scores is not None:
            sort_order = np.argsort(-scores[criteria])
            matches = matches.iloc[sort_order]

        return cast(CatalogFrame, matches)

    def find_one(self, *args: str | None, **kwargs: str | None) -> Table:
        """Find and load a single table matching search criteria.

        Convenience method that combines find() and load(). Requires exactly
        one matching table.

        Args:
            *args: Positional arguments passed to find().
            **kwargs: Keyword arguments passed to find().

        Returns:
            The loaded Table object.

        Raises:
            ValueError: If zero or multiple tables match the criteria.

        Example:
            Load a specific table
            ```python
            from owid.catalog import RemoteCatalog

            catalog = RemoteCatalog()
            table = catalog.find_one(table="population", namespace="un")
            ```
        """
        return self.find(*args, **kwargs).load()  # type: ignore

    def find_latest(
        self,
        *args: str | None,
        **kwargs: str | None,
    ) -> Table:
        """Find and load the latest version of a table.

        Searches for tables matching the criteria and returns the one with
        the most recent version string (lexicographically sorted).

        Args:
            *args: Positional arguments passed to find().
            **kwargs: Keyword arguments passed to find().

        Returns:
            The loaded Table with the latest version.

        Raises:
            ValueError: If no tables match the criteria.

        Example:
            Get latest population data
            ```python
            from owid.catalog import find_latest

            table = find_latest(table="population", namespace="un")
            print(f"Loaded version: {table.metadata.version}")
            ```
        """
        frame = self.find(*args, **kwargs)  # type: ignore
        if frame.empty:
            raise ValueError("No matching table found")
        else:
            return cast(Table, frame.sort_values("version").iloc[-1].load())

    def __getitem__(self, path: str) -> Table:
        uri = "/".join([self.uri.rstrip("/"), path])
        for _format in SUPPORTED_FORMATS:
            try:
                return Table.read(f"{uri}.{_format}")
            except Exception:
                continue

        raise KeyError(f"no matching table found at: {uri}")


class LocalCatalog(CatalogMixin):
    """Local filesystem-based data catalog.

    Provides access to datasets stored on disk. Can operate without an index file
    by walking the directory structure, or use a pre-built index for faster queries.

    The catalog automatically builds an index on first access if one doesn't exist.
    Use the `reindex()` method to rebuild or update the index.

    Attributes:
        uri: Path to the catalog directory on disk.
        channels: Data channels available in this catalog.
        frame: Indexed catalog contents as a DataFrame.

    Example:
        Create and use a local catalog
        ```python
        from pathlib import Path
        from owid.catalog import LocalCatalog

        # Initialize catalog
        catalog = LocalCatalog(Path("./data"), channels=["garden", "meadow"])

        # Search for tables
        results = catalog.find(table="population")

        # Load specific table
        table = catalog.find_one(namespace="un", table="population")
        ```
    """

    uri: str

    def __init__(self, path: str | Path, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = str(path)
        self.channels = channels
        if self._catalog_exists(channels):
            self.frame = CatalogFrame(self._read_channels(channels))
            self.frame._base_uri = self.path.as_posix() + "/"
        else:
            # could take a while to generate if there are many datasets
            self.reindex()

        # ensure the frame knows where to load data from

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
        """
        Read selected channels from local path.
        """
        df = pd.concat([read_frame(self._catalog_channel_file(channel)) for channel in channels])
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
        """Rebuild the catalog index by scanning the directory tree.

        Walks the directory structure to discover all datasets and tables,
        then generates and saves a new index file. This can take a while for
        large catalogs with many datasets.

        Args:
            include: Optional regex pattern to filter which datasets to include
                in the index. If specified, only matching datasets are scanned
                and merged with existing index entries.

        Example:
            Rebuild entire catalog index
            ```python
            catalog = LocalCatalog("./data")
            catalog.reindex()
            ```

            Reindex only specific namespace
            ```python
            catalog = LocalCatalog("./data")
            catalog.reindex(include="worldbank")
            ```
        """
        index = self._scan_for_datasets(include)

        if include:
            # we used regex to find datasets, so merge it with the original frame
            index = self._merge_index(self.frame, index)

        index._base_uri = self.path.as_posix() + "/"

        # convert int versions to strings
        index.version = index.version.astype(str)

        # make sure dimensions json is loaded
        index.dimensions = index.dimensions.map(lambda s: json.loads(s) if isinstance(s, str) else s)

        self._save_index(index)
        self.frame = index

    @staticmethod
    def _merge_index(frame: "CatalogFrame", update: "CatalogFrame") -> "CatalogFrame":
        """Merge two indexes."""
        return CatalogFrame(
            pd.concat(
                [update, frame.loc[~frame.path.isin(update.path)]],
                ignore_index=True,
            )
        )

    def _save_index(self, frame: "CatalogFrame") -> None:
        """
        Save all channels to disk in separate catalog files, and in each of our
        supported formats.
        """
        for channel in self.channels:
            channel_frame = frame.loc[frame.channel == channel].reset_index(drop=True)
            for format in INDEX_FORMATS:
                filename = self._catalog_channel_file(channel, format)
                save_frame(channel_frame, filename)

        # add a catalog version number that we can use to tell old clients to update
        self._save_metadata({"format_version": OWID_CATALOG_VERSION})

    def _scan_for_datasets(self, include: str | None = None) -> "CatalogFrame":
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

        return CatalogFrame(df)

    def _save_metadata(self, contents: dict[str, Any]) -> None:
        with open(self._metadata_file, "w") as ostream:
            json.dump(contents, ostream, indent=2)


class RemoteCatalog(CatalogMixin):
    """Remote HTTP-based data catalog.

    Provides access to datasets hosted on a remote server. The catalog downloads
    and caches index files on initialization for fast querying. Individual tables
    are downloaded on-demand when accessed.

    By default, connects to the public Our World in Data catalog at
    https://catalog.ourworldindata.org/.

    Attributes:
        uri: Base URI of the remote catalog server.
        channels: Data channels available in this catalog.
        frame: Cached catalog index as a DataFrame.
        metadata: Catalog metadata including format version.

    Example:
        Use the default remote catalog
        ```python
        from owid.catalog import RemoteCatalog

        # Connect to default catalog
        catalog = RemoteCatalog()

        # Search and load data
        results = catalog.find(table="gdp")
        table = results.iloc[0].load()
        ```

        Connect to custom remote catalog
        ```python
        catalog = RemoteCatalog(
            uri="https://custom-catalog.example.com/",
            channels=["garden", "meadow"]
        )
        ```
    """

    uri: str

    def __init__(self, uri: str = OWID_CATALOG_URI, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = uri
        self.channels = channels
        self.metadata = self._read_metadata(self.uri + "catalog.meta.json")
        if self.metadata["format_version"] > OWID_CATALOG_VERSION:
            raise PackageUpdateRequired(
                f"library supports api version {OWID_CATALOG_VERSION}, "
                f"but the remote catalog has version {self.metadata['format_version']} "
                "-- please update"
            )

        self.frame = CatalogFrame(self._read_channels(uri, channels))
        self.frame._base_uri = uri

    @property
    def datasets(self) -> pd.DataFrame:
        return self.frame[["namespace", "version", "dataset"]].drop_duplicates()

    @staticmethod
    def _read_metadata(uri: str) -> dict[str, Any]:
        """
        Read the metadata JSON blob for this repo.
        """
        resp = requests.get(uri)
        resp.raise_for_status()
        return cast(dict[str, Any], resp.json())

    @staticmethod
    def _read_channels(uri: str, channels: Iterable[CHANNEL]) -> pd.DataFrame:
        """
        Read selected channels from S3.
        """
        return pd.concat([read_frame(uri + f"catalog-{channel}.{PREFERRED_FORMAT}") for channel in channels])


class CatalogFrame(pd.DataFrame):
    """DataFrame of catalog search results.

    Extended pandas DataFrame that represents catalog search results.
    Each row contains metadata about a table, including namespace, version,
    and path information. The DataFrame can load tables directly via the
    `load()` method.

    Attributes:
        _base_uri: Base URI for loading table data.

    Example:
        Working with search results
        ```python
        from owid.catalog import find

        # Search returns a CatalogFrame
        results = find(table="population")
        print(results)

        # Load first matching table
        table = results.iloc[0].load()

        # Or load if exactly one match
        table = results.load()
        ```
    """

    _base_uri: str | None = None

    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogFrame

    @property
    def _constructor_sliced(self) -> Any:
        # ensure that when we pick a series we still have the URI
        def build(*args: Any, **kwargs: Any) -> Any:
            c = CatalogSeries(*args, **kwargs)
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
    def create_empty() -> "CatalogFrame":
        return CatalogFrame(
            {
                "namespace": [],
                "version": [],
                "table": [],
                "dimensions": [],
                "path": [],
                "format": [],
            }
        )


class CatalogSeries(pd.Series):
    """Single catalog search result row.

    Extended pandas Series representing one table in the catalog. Contains
    metadata fields like namespace, version, dataset, and path. Provides a
    `load()` method to download and load the actual table data.

    Attributes:
        _base_uri: Base URI for loading table data.
        namespace: Data provider namespace (e.g., 'un', 'worldbank').
        version: Dataset version string (e.g., '2024-01-15').
        dataset: Dataset name.
        table: Table name within the dataset.
        path: Relative path to the table file.

    Example:
        Load table from search result
        ```python
        from owid.catalog import find

        results = find(table="population")
        row = results.iloc[0]

        # Inspect metadata
        print(f"{row.namespace}/{row.version}/{row.dataset}")

        # Load the table
        table = row.load()
        ```
    """

    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogSeries

    def load(self) -> Table:
        # determine what format to use for this table; old indexes gave one format,
        # new ones give multiple to choose from
        format = None
        if hasattr(self, "format"):
            # backwards compatibility with existing indexes
            format = self.format
        elif hasattr(self, "formats") and (self.formats is not None) and len(self.formats) > 0:
            format = PREFERRED_FORMAT if PREFERRED_FORMAT in self.formats else self.formats[0]

        if self.path and format and self._base_uri:
            with tempfile.TemporaryDirectory() as tmpdir:
                uri = self._base_uri + self.path + "." + format

                # download the data locally first if the file is private
                # keep backward compatibility
                if not getattr(self, "is_public", True):
                    uri = _download_private_file(uri, tmpdir)

                return Table.read(uri)

        raise ValueError("series is not a table spec")


def _load_remote_catalog(channels):
    global REMOTE_CATALOG

    # add channel if missing and reinit remote catalog
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
    case: bool = False,
    regex: bool = True,
    fuzzy: bool = False,
    threshold: int = 70,
) -> "CatalogFrame":
    """Search the remote catalog for tables matching criteria.

    Convenience function that searches the default Our World in Data remote
    catalog. Automatically initializes and caches the catalog connection.

    Args:
        table: Table name pattern to search for (regex by default).
        namespace: Namespace to filter by (e.g., 'un', 'worldbank').
        version: Version string to filter by (e.g., '2024-01-15').
        dataset: Dataset name pattern to filter by (regex by default).
        channels: Data channels to search (default: garden only).
        case: If True, search is case-sensitive. Default False.
        regex: If True, treat table/dataset as regex patterns. Default True.
            Ignored when fuzzy=True.
        fuzzy: If True, use fuzzy matching for table/dataset. Default False.
            When enabled, matches strings that are similar but not exact.
        threshold: Minimum fuzzy match score (0-100). Default 70.
            Only used when fuzzy=True. Higher values require closer matches.

    Returns:
        CatalogFrame containing matching tables.

    Example:
        Search for population data
        ```python
        from owid.catalog import find

        # Find all population tables (case-insensitive regex)
        results = find(table="population")

        # Filter by namespace
        results = find(table="population", namespace="un")

        # Search across multiple channels
        results = find(table="gdp", channels=["garden", "meadow"])

        # Case-sensitive literal search
        results = find(table="GDP", case=True, regex=False)

        # Fuzzy search - matches similar strings
        results = find(table="gdp per capita", fuzzy=True)
        results = find(dataset="wrld bank", fuzzy=True, threshold=60)
        ```
    """
    REMOTE_CATALOG = _load_remote_catalog(channels=channels)

    return REMOTE_CATALOG.find(
        table=table,
        namespace=namespace,
        version=version,
        dataset=dataset,
        case=case,
        regex=regex,
        fuzzy=fuzzy,
        threshold=threshold,
    )


def find_one(*args: str | None, **kwargs: str | None) -> Table:
    """Find and load a single table from the remote catalog.

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
        Load a specific table
        ```python
        from owid.catalog import find_one

        # Load exact match
        table = find_one(
            table="population",
            namespace="un",
            version="2024-07-11"
        )
        ```
    """
    return find(*args, **kwargs).load()  # type: ignore


def find_latest(
    table: str | None = None,
    namespace: str | None = None,
    dataset: str | None = None,
    channels: Iterable[CHANNEL] = ("garden",),
    version: str | None = None,
) -> Table:
    """Find and load the latest version of a table from the remote catalog.

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
        Get latest population data
        ```python
        from owid.catalog import find_latest

        # Load most recent version
        table = find_latest(table="population", namespace="un")
        print(f"Loaded version: {table.m.version}")
        ```

        Load from multiple channels
        ```python
        table = find_latest(
            table="gdp",
            namespace="worldbank",
            channels=["garden", "meadow"]
        )
        ```
    """
    REMOTE_CATALOG = _load_remote_catalog(channels=channels)

    # If version is not specified, it will find the latest version given all other specifications.
    return REMOTE_CATALOG.find_latest(table=table, namespace=namespace, dataset=dataset, version=version)


def find_by_indicator(query: str, limit: int = 10) -> CatalogFrame:
    """Search for tables by indicator name using semantic search.

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
        from owid.catalog import find_by_indicator

        # Search for indicators
        results = find_by_indicator("solar power")
        print(results[["indicator_title", "indicator", "dataset"]])

        # Load the table containing the top result
        table = results.iloc[0].load()
        ```
    """
    resp = requests.get(
        OWID_SEARCH_API,
        params={"query": query, "limit": limit},
    )
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])

    # Build rows for CatalogFrame
    rows = []
    for r in results:
        catalog_path = r.get("catalog_path", "")

        # Parse catalog_path: "grapher/namespace/version/dataset/table#column"
        # Example: "grapher/unep/2023-12-12/renewable_energy/investments#solar"
        path_part, _, indicator = catalog_path.partition("#")
        parts = path_part.split("/")

        if len(parts) >= 4:
            channel, namespace, version, dataset = parts[0], parts[1], parts[2], parts[3]
            table = parts[4] if len(parts) > 4 else dataset
        else:
            # Fallback if parsing fails - use pd.NA for missing values
            channel, namespace, version, dataset, table = pd.NA, pd.NA, pd.NA, pd.NA, pd.NA
            path_part = pd.NA
            indicator = indicator if indicator else pd.NA

        rows.append(
            {
                # Indicator-specific columns first
                "indicator_title": r.get("title"),
                "indicator": indicator if indicator else pd.NA,
                "score": r.get("score"),
                # Standard catalog columns in consistent order
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

    # Enrich with dimensions from the remote catalog
    if not frame.empty:
        catalog = _load_remote_catalog(channels=["grapher"])
        # Merge on path to get dimensions
        frame = frame.merge(
            catalog.frame[["path", "dimensions"]],
            on="path",
            how="left",
        )
        # Reorder columns to put dimensions in the right place
        cols = [c for c in frame.columns if c != "dimensions"]
        cols.insert(cols.index("is_public") + 1, "dimensions")
        frame = CatalogFrame(frame[cols])
        frame._base_uri = OWID_CATALOG_URI

    return frame


def _download_private_file(uri: str, tmpdir: str) -> str:
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


class PackageUpdateRequired(Exception):
    pass


def read_frame(uri: str | Path) -> pd.DataFrame:
    if isinstance(uri, Path):
        uri = str(uri)

    if uri.endswith(".feather"):
        return cast(pd.DataFrame, pd.read_feather(uri))

    elif uri.endswith(".parquet"):
        return cast(pd.DataFrame, pd.read_parquet(uri))

    elif uri.endswith(".csv"):
        return pd.read_csv(uri)

    raise ValueError(f"could not detect format of uri: {uri}")


def save_frame(df: pd.DataFrame, path: str | Path) -> None:
    path = str(path)
    if path.endswith(".feather"):
        df.to_feather(path)

    elif path.endswith(".parquet"):
        df.to_parquet(path)

    elif path.endswith(".csv"):
        df.to_csv(path)

    else:
        raise ValueError(f"could not detect what format to write to: {path}")


def _match_score(series: pd.Series, query: str, fuzzy: bool, case: bool, regex: bool) -> npt.NDArray[np.float64]:
    """Calculate match scores for a text column.

    Args:
        series: The pandas Series to search.
        query: The search query string.
        fuzzy: If True, use fuzzy matching (0-100 scores).
        case: If True, matching is case-sensitive.
        regex: If True, treat query as regex (ignored when fuzzy=True).

    Returns:
        Array of match scores: 0-100 for fuzzy, 0 or 100 for regex/literal.
    """
    if fuzzy:
        if not case:
            query = query.lower()
            series = series.str.lower()
        return np.array([fuzz.WRatio(query, x) for x in series], dtype=np.float64)
    else:
        matches = series.str.contains(query, case=case, regex=regex)
        return np.where(matches, 100.0, 0.0)
