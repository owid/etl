#
#  owid.catalog.api.catalogs
#
#  Catalog classes for finding and loading data.
#
from __future__ import annotations

import heapq
import json
import os
import re
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt
import pandas as pd
import requests
import structlog
from rapidfuzz import fuzz

from owid.catalog import s3_utils
from owid.catalog.api.utils import (
    INDEX_FORMATS,
    OWID_CATALOG_URI,
    OWID_CATALOG_VERSION,
    PREFERRED_FORMAT,
    S3_OWID_URI,
    SUPPORTED_FORMATS,
)
from owid.catalog.datasets import CHANNEL, Dataset, FileFormat
from owid.catalog.tables import Table

log = structlog.get_logger()


def download_private_file_s3(uri: str, tmpdir: str) -> str:
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


def read_frame(uri: str | Path) -> pd.DataFrame:
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


def save_frame(df: pd.DataFrame, path: str | Path) -> None:
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


def _match_score(
    series: pd.Series,
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


class CatalogMixin:
    """Abstract catalog interface for finding and loading data."""

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
        match: Literal["exact", "contains", "regex", "fuzzy"] = "exact",
        fuzzy_threshold: int = 70,
    ) -> "CatalogFrame":
        """Search catalog for tables matching specified criteria.

        Args:
            table: Table name pattern
            namespace: Namespace filter (exact match only)
            version: Version filter (exact match only)
            dataset: Dataset name pattern
            channel: Channel filter (exact match only)
            case: Case-sensitive search (default: False)
            match: How to match table/dataset names (default: "exact"):
                - "exact": Exact string match
                - "contains": Substring match
                - "regex": Regular expression pattern
                - "fuzzy": Typo-tolerant similarity matching
            fuzzy_threshold: Minimum similarity score 0-100 for fuzzy matching.
                Only used when match="fuzzy". (default: 70)

        Returns:
            CatalogFrame with matching tables, sorted by relevance if match="fuzzy".

        Example:
            ```python
            # Exact match (default)
            catalog.find(table="population")

            # Substring search
            catalog.find(table="pop", match="contains")

            # Regex pattern
            catalog.find(table="pop.*density", match="regex")

            # Fuzzy typo-tolerant search
            catalog.find(table="populaton", match="fuzzy", fuzzy_threshold=80)
            ```
        """
        criteria: npt.ArrayLike = np.ones(len(self.frame), dtype=bool)
        scores: npt.NDArray[np.float64] | None = None

        # Table matching with scoring
        if table:
            table_scores = _match_score(self.frame.table, table, match, case, fuzzy_threshold)
            if match == "fuzzy":
                criteria &= table_scores >= fuzzy_threshold
                scores = table_scores
            else:
                criteria &= table_scores > 0

        # Dataset matching with scoring
        if dataset:
            dataset_scores = _match_score(self.frame.dataset, dataset, match, case, fuzzy_threshold)
            if match == "fuzzy":
                criteria &= dataset_scores >= fuzzy_threshold
                # Average scores if both table and dataset specified
                scores = dataset_scores if scores is None else (scores + dataset_scores) / 2
            else:
                criteria &= dataset_scores > 0

        # Exact match filters
        if namespace:
            criteria &= self.frame.namespace == namespace
        if version:
            criteria &= self.frame.version == version
        if channel:
            if channel not in self.channels:
                raise ValueError(
                    f"You need to add `{channel}` to channels in Catalog init "
                    f"(only `{self.channels}` are loaded now)"
                )
            criteria &= self.frame.channel == channel

        matches = self.frame[criteria]
        if "checksum" in matches.columns:
            matches = matches.drop(columns=["checksum"])

        # Sort by relevance if fuzzy matching was used
        if match == "fuzzy" and scores is not None:
            sort_order = np.argsort(-scores[criteria])
            matches = matches.iloc[sort_order]

        return cast(CatalogFrame, matches)

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
    """Local filesystem catalog."""

    uri: str

    def __init__(self, path: str | Path, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = str(path)
        self.channels = channels
        if self._catalog_exists(channels):
            self.frame = CatalogFrame(self._read_channels(channels))
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
    def _merge_index(frame: "CatalogFrame", update: "CatalogFrame") -> "CatalogFrame":
        """Merge two indexes."""
        return CatalogFrame(
            pd.concat(
                [update, frame.loc[~frame.path.isin(update.path)]],
                ignore_index=True,
            )
        )

    def _save_index(self, frame: "CatalogFrame") -> None:
        """Save all channels to disk in separate catalog files."""
        for channel in self.channels:
            channel_frame = frame.loc[frame.channel == channel].reset_index(drop=True)
            for format in INDEX_FORMATS:
                filename = self._catalog_channel_file(channel, cast(FileFormat, format))
                save_frame(channel_frame, filename)

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


class ETLCatalog(CatalogMixin):
    """Remote HTTP catalog for ETL data."""

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
        """Read the metadata JSON blob for this repo."""
        resp = requests.get(uri)
        resp.raise_for_status()
        return cast(dict[str, Any], resp.json())

    @staticmethod
    def _read_channels(uri: str, channels: Iterable[CHANNEL]) -> pd.DataFrame:
        """Read selected channels from S3."""
        return pd.concat([read_frame(uri + f"catalog-{channel}.{PREFERRED_FORMAT}") for channel in channels])


class CatalogFrame(pd.DataFrame):
    """DataFrame of catalog search results."""

    _base_uri: str | None = None
    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogFrame

    @property
    def _constructor_sliced(self) -> Any:
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
    """Single catalog search result row."""

    _metadata = ["_base_uri"]

    @property
    def _constructor(self) -> type:
        return CatalogSeries

    def load(self) -> Table:
        format = None
        if hasattr(self, "format"):
            format = self.format
        elif hasattr(self, "formats") and (self.formats is not None) and len(self.formats) > 0:
            format = PREFERRED_FORMAT if PREFERRED_FORMAT in self.formats else self.formats[0]

        if self.path and format and self._base_uri:
            with tempfile.TemporaryDirectory() as tmpdir:
                uri = self._base_uri + self.path + "." + format

                if not getattr(self, "is_public", True):
                    uri = download_private_file_s3(uri, tmpdir)

                return Table.read(uri)

        raise ValueError("series is not a table spec")


class PackageUpdateRequired(Exception):
    """Raised when catalog format version is newer than library version."""

    pass
