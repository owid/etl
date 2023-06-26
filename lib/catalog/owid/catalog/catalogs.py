#
#  catalog.py
#  owid-catalog-py
#

import heapq
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union, cast
from urllib.parse import urlparse

import numpy as np
import numpy.typing as npt
import pandas as pd
import requests
import structlog

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

# global copy cached after first request
REMOTE_CATALOG: Optional["RemoteCatalog"] = None

# what formats should we for our index of available datasets?
INDEX_FORMATS: List[FileFormat] = ["feather"]


class CatalogMixin:
    """
    Abstract data catalog API, encapsulates finding and loading data.
    """

    channels: Iterable[CHANNEL]
    frame: "CatalogFrame"
    uri: str

    def find(
        self,
        table: Optional[str] = None,
        namespace: Optional[str] = None,
        version: Optional[str] = None,
        dataset: Optional[str] = None,
        channel: Optional[CHANNEL] = None,
    ) -> "CatalogFrame":
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

        return cast(CatalogFrame, matches)

    def find_one(self, *args: Optional[str], **kwargs: Optional[str]) -> Table:
        return self.find(*args, **kwargs).load()  # type: ignore

    def find_latest(
        self,
        *args: Optional[str],
        **kwargs: Optional[str],
    ) -> Table:
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
    """
    A data catalog that's on disk. On-disk catalogs do not need an index file, since
    you can simply walk the directory. However, they support a `reindex()` method
    which can create such an index.
    """

    uri: str

    def __init__(self, path: Union[str, Path], channels: Iterable[CHANNEL] = ("garden",)) -> None:
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

    def iter_datasets(self, channel: CHANNEL, include: Optional[str] = None) -> Iterator[Dataset]:
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

    def reindex(self, include: Optional[str] = None) -> None:
        """
        Walk the directory tree, generate a channel/namespace/version/dataset/table frame
        and save it to each of our index formats.
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

    def _scan_for_datasets(self, include: Optional[str] = None) -> "CatalogFrame":
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

    def _save_metadata(self, contents: Dict[str, Any]) -> None:
        with open(self._metadata_file, "w") as ostream:
            json.dump(contents, ostream, indent=2)


class RemoteCatalog(CatalogMixin):
    uri: str

    def __init__(self, uri: str = OWID_CATALOG_URI, channels: Iterable[CHANNEL] = ("garden",)) -> None:
        self.uri = uri
        self.channels = channels
        self.metadata = self._read_metadata(self.uri + "catalog.meta.json")
        if self.metadata["format_version"] > OWID_CATALOG_VERSION:
            raise PackageUpdateRequired(
                f"library supports api version {OWID_CATALOG_VERSION}, "
                f'but the remote catalog has version {self.metadata["format_version"]} '
                "-- please update"
            )

        self.frame = CatalogFrame(self._read_channels(uri, channels))
        self.frame._base_uri = uri

    @property
    def datasets(self) -> pd.DataFrame:
        return self.frame[["namespace", "version", "dataset"]].drop_duplicates()

    @staticmethod
    def _read_metadata(uri: str) -> Dict[str, Any]:
        """
        Read the metadata JSON blob for this repo.
        """
        resp = requests.get(uri)
        resp.raise_for_status()
        return cast(Dict[str, Any], resp.json())

    @staticmethod
    def _read_channels(uri: str, channels: Iterable[CHANNEL]) -> pd.DataFrame:
        """
        Read selected channels from S3.
        """
        return pd.concat([read_frame(uri + f"catalog-{channel}.{PREFERRED_FORMAT}") for channel in channels])


class CatalogFrame(pd.DataFrame):
    """
    DataFrame helper, meant only for displaying catalog results.
    """

    _base_uri: Optional[str] = None

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
    """
    A row from the catalog representing a single dataset. We subclass Series
    in order to add a `load()` method onto it that will fetch and return a Table.
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
    table: Optional[str] = None,
    namespace: Optional[str] = None,
    version: Optional[str] = None,
    dataset: Optional[str] = None,
    channels: Iterable[CHANNEL] = ("garden",),
) -> "CatalogFrame":
    REMOTE_CATALOG = _load_remote_catalog(channels=channels)

    return REMOTE_CATALOG.find(table=table, namespace=namespace, version=version, dataset=dataset)


def find_one(*args: Optional[str], **kwargs: Optional[str]) -> Table:
    return find(*args, **kwargs).load()  # type: ignore


def find_latest(
    table: Optional[str] = None,
    namespace: Optional[str] = None,
    dataset: Optional[str] = None,
    channels: Iterable[CHANNEL] = ("garden",),
    version: Optional[str] = None,
) -> Table:
    REMOTE_CATALOG = _load_remote_catalog(channels=channels)

    # If version is not specified, it will find the latest version given all other specifications.
    return REMOTE_CATALOG.find_latest(table=table, namespace=namespace, dataset=dataset, version=version)


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


def read_frame(uri: Union[str, Path]) -> pd.DataFrame:
    if isinstance(uri, Path):
        uri = str(uri)

    if uri.endswith(".feather"):
        return cast(pd.DataFrame, pd.read_feather(uri))

    elif uri.endswith(".parquet"):
        return cast(pd.DataFrame, pd.read_parquet(uri))

    elif uri.endswith(".csv"):
        return pd.read_csv(uri)

    raise ValueError(f"could not detect format of uri: {uri}")


def save_frame(df: pd.DataFrame, path: Union[str, Path]) -> None:
    path = str(path)
    if path.endswith(".feather"):
        df.to_feather(path)

    elif path.endswith(".parquet"):
        df.to_parquet(path)

    elif path.endswith(".csv"):
        df.to_csv(path)

    else:
        raise ValueError(f"could not detect what format to write to: {path}")
