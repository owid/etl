#
#  owid.catalog.api.tables
#
#  Tables API for querying and loading tables from the OWID catalog.
#
from __future__ import annotations

import json
import os
import tempfile
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt
import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from rapidfuzz import fuzz

from owid.catalog import s3_utils
from owid.catalog.api.models import ResponseSet
from owid.catalog.api.utils import (
    OWID_CATALOG_VERSION,
    PREFERRED_FORMAT,
    S3_OWID_URI,
    SUPPORTED_FORMATS,
    _loading_data_from_api,
)
from owid.catalog.core import CatalogPath
from owid.catalog.core.paths import VALID_CHANNELS
from owid.catalog.core.tables import Table

if TYPE_CHECKING:
    from owid.catalog.api import Client


# =============================================================================
# Catalog Index Utilities
# =============================================================================


def _download_private_file_s3(uri: str, tmpdir: str) -> str:
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


class CatalogVersionError(Exception):
    """Raised when catalog format version is newer than library version."""

    pass


def _read_catalog_index(uri: str, *, timeout: int = 30) -> pd.DataFrame:
    """Read catalog index from remote URI.

    Args:
        uri: Base URI for the catalog (e.g., "https://catalog.ourworldindata.org/")
        timeout: HTTP request timeout in seconds.

    Returns:
        DataFrame with catalog index.

    Raises:
        CatalogVersionError: If catalog format version is newer than library version.
    """
    # Read metadata to check version
    metadata_url = uri.rstrip("/") + "/catalog.meta.json"
    resp = requests.get(metadata_url, timeout=timeout)
    resp.raise_for_status()
    metadata = resp.json()

    if metadata["format_version"] > OWID_CATALOG_VERSION:
        raise CatalogVersionError(
            f"Library supports catalog version {OWID_CATALOG_VERSION}, "
            f"but the remote catalog has version {metadata['format_version']} "
            "-- please update owid-catalog"
        )

    # Read all channels
    frames = []
    for channel in VALID_CHANNELS:
        index_url = f"{uri.rstrip('/')}/catalog-{channel}.{PREFERRED_FORMAT}"
        try:
            df = cast(pd.DataFrame, pd.read_feather(index_url))
            frames.append(df)
        except Exception:
            # Channel might not exist, skip it
            continue

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def _match_score(
    series: pd.Series,  # type: ignore[type-arg]
    query: str,
    mode: Literal["exact", "contains", "regex", "fuzzy"],
    case: bool,
    threshold: int = 70,
) -> npt.NDArray[np.float64]:
    """Calculate match scores for a text column based on search mode.

    Args:
        series: Text column to search
        query: Search pattern
        mode: Matching mode - "exact", "contains", "regex", or "fuzzy"
        case: Case-sensitive matching
        threshold: Score cutoff for fuzzy matching (only used when mode="fuzzy")

    Returns:
        Array of match scores: 0-100 for fuzzy, 0 or 100 for other modes.
    """
    if mode == "fuzzy":
        # Fuzzy matching with similarity scoring (0-100)
        if not case:
            query = query.lower()
            series = series.str.lower()
        return np.array([fuzz.WRatio(query, x, score_cutoff=threshold) for x in series], dtype=np.float64)
    elif mode == "regex":
        # Regex pattern matching (0 or 100)
        matches = series.str.contains(query, regex=True, case=case, na=False)
        return np.where(matches, 100.0, 0.0)
    elif mode == "contains":
        # Substring matching (0 or 100)
        matches = series.str.contains(query, regex=False, case=case, na=False)
        return np.where(matches, 100.0, 0.0)
    else:  # mode == "exact"
        # Exact string matching (0 or 100)
        if case:
            matches = series == query
        else:
            matches = series.str.lower() == query.lower()
        return np.where(matches, 100.0, 0.0)


# =============================================================================
# Table Loading
# =============================================================================


def _load_table(
    path: str,
    *,
    catalog_url: str,
    formats: list[str] | None = None,
    is_public: bool = True,
    load_data: bool = True,
) -> Table:
    """Load a table from the catalog by path.

    Helper function for loading table data. Used by TableResult and IndicatorResult.

    Args:
        path: Table path in catalog (e.g., "grapher/namespace/version/dataset/table")
        catalog_url: Base URL for the catalog (required).
        formats: List of formats to try. If None, tries all supported formats.
        is_public: Whether the table is publicly accessible.
        load_data: If True, load full data. If False, load only table structure (columns and metadata) without rows.

    Returns:
        Table object with data and metadata (or just metadata if load_data=False).

    Raises:
        KeyError: If no table found at the specified path.
    """
    # Extract table name for display
    catalog_path = CatalogPath.from_str(path)
    table_name = catalog_path.table or path
    message = f"Loading table '{table_name}'"

    def fct():
        base_uri = catalog_url
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
                    tmpdir = tempfile.mkdtemp()
                    table_uri = _download_private_file_s3(table_uri, tmpdir)

                # If header_only, return empty table with same structure
                return Table.read(table_uri, load_data=load_data)
            except Exception:
                continue

        raise KeyError(f"No matching table found at: {path}")

    if load_data:
        with _loading_data_from_api(message):
            return fct()
    else:
        return fct()


class TableResult(BaseModel):
    """A table found in the catalog.

    Attributes:
        table: Table name.
        path: Full path to the table.
        channel: Data channel (garden, meadow, etc.).
        namespace: Data provider namespace.
        version: Version string.
        dataset: Dataset name.
        dimensions: List of dimension columns.
        is_public: Whether the data is publicly accessible.
        formats: List of available formats.
        popularity: Popularity score (0.0 to 1.0) based on analytics views.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    # Identification
    table: str

    # Location
    path: str

    # Structural metadata
    channel: str
    namespace: str
    version: str
    dataset: str

    # Content metadata
    dimensions: list[str] = Field(default_factory=list)

    # Technical metadata
    is_public: bool = True
    formats: list[str] = Field(default_factory=list)

    # Usage metadata
    popularity: float = 0.0

    # API configuration (immutable)
    catalog_url: str = Field(frozen=True)

    _cached_table: Table | None = PrivateAttr(default=None)

    def fetch(self, *, load_data: bool = True) -> Table:
        """Fetch table data.

        Args:
            load_data: If True (default), load full table data.
                       If False, load only structure (columns and metadata) without rows.

        Returns:
            Table with data and metadata (or just metadata if load_data=False).

        Example:
            ```python
            result = client.tables.search(table="population")[0]
            tb = result.fetch()
            print(tb.head())
            print(tb.columns)
            ```
        """
        # Return cached if available and requesting full data
        if load_data and self._cached_table is not None:
            return self._cached_table

        tb = _load_table(
            self.path,
            formats=self.formats,
            is_public=self.is_public,
            load_data=load_data,
            catalog_url=self.catalog_url,
        )

        # Cache only if loading full data
        if load_data:
            self._cached_table = tb

        return tb


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
        table = results[0].fetch()

        # Fetch table directly by path
        tb = client.tables.fetch("garden/un/2024-07-12/un_wpp/population")
        print(tb.head())
        ```
    """

    def __init__(self, client: "Client", catalog_url: str) -> None:
        """Initialize the TablesAPI.

        Args:
            client: The Client instance.
            catalog_url: Base URL for the catalog (e.g., "https://catalog.ourworldindata.org/").
        """
        self._client = client
        self._catalog_url = catalog_url
        self._index: pd.DataFrame | None = None

    @property
    def catalog_url(self) -> str:
        """Base URL for the catalog (read-only)."""
        return self._catalog_url

    def _get_index(self, timeout: int | None = None, *, force: bool = False) -> pd.DataFrame:
        """Get or load the catalog index.

        Args:
            timeout: HTTP request timeout in seconds. Defaults to client timeout.
                     Only used during index loading.
            force: If True, force re-download even if cached.

        Returns:
            DataFrame with catalog index.
        """
        if self._index is None or force:
            self._index = _read_catalog_index(
                self.catalog_url,
                timeout=timeout or self._client.timeout,
            )

        return self._index

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

    def _filter_index(
        self,
        index: pd.DataFrame,
        *,
        table: str | None,
        dataset: str | None,
        namespace: str | None,
        version: str | None,
        channel: str,
        match: Literal["exact", "contains", "regex", "fuzzy"],
        case: bool,
        fuzzy_threshold: int,
    ) -> tuple[pd.DataFrame, npt.NDArray[np.float64] | None]:
        """Apply search criteria to index, return filtered matches and optional scores.

        Args:
            index: Catalog index DataFrame.
            table: Table name pattern to search for.
            dataset: Dataset name pattern to search for.
            namespace: Filter by namespace (exact match).
            version: Filter by version (exact match).
            channel: Filter by channel (exact match).
            match: Matching mode for table/dataset patterns.
            case: Case-sensitive matching.
            fuzzy_threshold: Minimum score for fuzzy matching.

        Returns:
            Tuple of (filtered DataFrame, optional fuzzy scores array).
        """
        criteria: npt.ArrayLike = np.ones(len(index), dtype=bool)
        scores: npt.NDArray[np.float64] | None = None

        # Table matching with scoring
        if table:
            table_scores = _match_score(index["table"], table, match, case, fuzzy_threshold)
            if match == "fuzzy":
                criteria &= table_scores >= fuzzy_threshold
                scores = table_scores
            else:
                criteria &= table_scores > 0

        # Dataset matching with scoring
        if dataset:
            dataset_scores = _match_score(index["dataset"], dataset, match, case, fuzzy_threshold)
            if match == "fuzzy":
                criteria &= dataset_scores >= fuzzy_threshold
                # Average scores if both table and dataset specified
                scores = dataset_scores if scores is None else (scores + dataset_scores) / 2
            else:
                criteria &= dataset_scores > 0

        # Exact match filters
        if namespace:
            criteria &= index["namespace"] == namespace
        if version:
            criteria &= index["version"] == version
        if channel:
            criteria &= index["channel"] == channel

        matches = index[criteria]

        # Drop checksum column if present
        if "checksum" in matches.columns:
            matches = matches.drop(columns=["checksum"])

        # Sort by relevance if fuzzy matching was used
        if match == "fuzzy" and scores is not None:
            sort_order = np.argsort(-scores[criteria])
            matches = matches.iloc[sort_order]

        return matches, scores[criteria] if scores is not None else None

    def _fetch_popularity(
        self,
        matches: pd.DataFrame,
        timeout: int | None,
    ) -> dict[str, float]:
        """Fetch popularity data for matched datasets.

        Args:
            matches: Filtered DataFrame of matching tables.
            timeout: HTTP request timeout in seconds.

        Returns:
            Dict mapping dataset slugs to popularity scores.
        """
        if matches.empty:
            return {}

        # Vectorized slug creation (much faster than iterrows)
        dataset_slugs = (
            matches["namespace"] + "/" + matches["version"].astype(str) + "/" + matches["dataset"]
        ).tolist()

        return self._client._datasette.fetch_popularity(
            sorted(set(dataset_slugs)),
            type="dataset",
            timeout=timeout or self._client.timeout,
        )

    def _to_results(
        self,
        matches: pd.DataFrame,
        popularity: dict[str, float],
    ) -> list[TableResult]:
        """Convert DataFrame matches to TableResult objects.

        Args:
            matches: Filtered DataFrame of matching tables.
            popularity: Dict mapping dataset slugs to popularity scores.

        Returns:
            List of TableResult objects sorted by popularity.
        """
        results = []

        # Use to_dict("records") for better performance than iterrows
        for row in matches.to_dict("records"):
            # Handle dimensions - could be list or JSON string
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

            slug = f"{row['namespace']}/{row['version']}/{row['dataset']}"
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
                    popularity=popularity.get(slug, 0.0),
                    catalog_url=self.catalog_url,
                )
            )

        # Sort by popularity (descending) - most popular first
        results.sort(key=lambda r: r.popularity, reverse=True)
        return results

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
        refresh_index: bool = False,
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
                - "fuzzy": Typo-tolerant similarity matching
                - "exact": Exact string match
                - "contains": Substring match
                - "regex": Regular expression pattern
            fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching.
                Only used when match="fuzzy". (default: 70)
            timeout: HTTP request timeout in seconds for catalog loading. Defaults to client timeout.
            refresh_index: If True, force re-download of the catalog index. Default False.

        Returns:
            ResponseSet containing matching TableResult objects, sorted by popularity (most viewed first).
            If match="fuzzy", results are sorted by fuzzy relevance score instead.
            Each result includes a `popularity` field (0.0-1.0) based on analytics views.

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
            tb = results[0].fetch()
            ```
        """
        # Default to garden channel if not specified
        if channel is None:
            channel = "garden"

        # Load and filter catalog index
        index = self._get_index(timeout=timeout, force=refresh_index)
        matches, _ = self._filter_index(
            index,
            table=table,
            dataset=dataset,
            namespace=namespace,
            version=version,
            channel=channel,
            match=match,
            case=case,
            fuzzy_threshold=fuzzy_threshold,
        )

        # Fetch popularity data
        popularity = self._fetch_popularity(matches, timeout)

        # Convert to results
        results = self._to_results(matches, popularity)

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
            base_url=self.catalog_url,
        )

    def fetch(
        self,
        path: str,
        *,
        load_data: bool = True,
        formats: list[str] | None = None,
        is_public: bool = True,
        timeout: int | None = None,
    ) -> Table:
        """Fetch a table by catalog path.

        Loads the table directly from the catalog.

        Args:
            path: Full catalog path (e.g., "garden/un/2024-07-12/un_wpp/population").
            load_data: If True (default), load full table data.
                       If False, load only table structure (columns and metadata) without rows.
            formats: List of formats to try. If None, tries all supported formats.
            is_public: Whether the table is publicly accessible. Default True.
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

        return _load_table(
            path, formats=formats, is_public=is_public, load_data=load_data, catalog_url=self.catalog_url
        )
