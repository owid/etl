import datetime as dt
import json
import re
import shutil
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import yaml
from dataclasses_json import dataclass_json
from owid.catalog.meta import DatasetMeta, License, Source, TableMeta, pruned_json
from owid.datautils import dataframes
from owid.walden import files
from tenacity import Retrying
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

from etl import paths
from etl.files import yaml_dump

dvc = None

# DVC is not thread-safe, so we need to lock it
dvc_lock = Lock()
unignore_backports_lock = Lock()


def get_dvc():
    from dvc.repo import Repo

    global dvc

    if dvc is None:
        dvc = Repo(paths.BASE_DIR)

    return dvc


@dataclass
class Snapshot:
    uri: str
    metadata: "SnapshotMeta"

    def __init__(self, uri: str) -> None:
        """
        :param uri: URI of the snapshot file, typically `namespace/version/short_name.ext`
        """
        self.uri = uri

        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Metadata file {self.metadata_path} not found")

        self.metadata = SnapshotMeta.load_from_yaml(self.metadata_path)

    @property
    def path(self) -> Path:
        """Path to materialized file."""
        return paths.DATA_DIR / "snapshots" / self.uri

    @property
    def metadata_path(self) -> Path:
        """Path to metadata file."""
        return Path(f"{paths.SNAPSHOTS_DIR / self.uri}.dvc")

    def pull(self, force=True) -> None:
        """Pull file from S3."""
        with _unignore_backports(self.path):
            get_dvc().pull(str(self.path), remote="public-read" if self.metadata.is_public else "private", force=force)

    def delete_local(self) -> None:
        """Delete local file and its metadata."""
        if self.path.exists():
            self.path.unlink()
        if self.metadata_path.exists():
            self.metadata_path.unlink()

    def download_from_source(self) -> None:
        """Download file from source_data_url."""
        assert self.metadata.source.source_data_url, "source_data_url is not set"
        self.path.parent.mkdir(exist_ok=True, parents=True)
        files.download(self.metadata.source.source_data_url, str(self.path))

    def dvc_add(self, upload: bool) -> None:
        """Add file to DVC and upload to S3."""
        from dvc.exceptions import UploadError

        dvc = get_dvc()

        with dvc_lock, _unignore_backports(self.path):
            dvc.add(str(self.path), fname=str(self.metadata_path))
            if upload:
                # DVC sometimes returns UploadError, retry a few times
                for attempt in Retrying(
                    stop=stop_after_attempt(3),
                    retry=retry_if_exception_type(UploadError),
                ):
                    with attempt:
                        dvc.push(str(self.path), remote="public" if self.metadata.is_public else "private")

    def to_table_metadata(self):
        table_meta = TableMeta.from_dict(
            {
                "short_name": self.metadata.short_name,
                "title": self.metadata.name,
                "description": self.metadata.description,
                "dataset": DatasetMeta.from_dict(
                    {
                        "channel": "snapshots",
                        "description": self.metadata.description,
                        "is_public": self.metadata.is_public,
                        "namespace": self.metadata.namespace,
                        "short_name": self.metadata.short_name,
                        "title": self.metadata.name,
                        "version": self.metadata.version,
                        "sources": [self.metadata.source],
                        "licenses": [self.metadata.license],
                    }
                ),
            }
        )
        return table_meta


@pruned_json
@dataclass_json
@dataclass
class SnapshotMeta:
    # how we identify the dataset, determined automatically from snapshot path
    namespace: str  # a short source name (usually institution name)
    version: str  # date, `latest` or year (discouraged)
    short_name: str  # a slug, ideally unique, snake_case, no spaces

    # how to get the data file
    file_extension: str

    # fields that are meant to be shown to humans
    name: str
    description: str

    source: Source
    license: Optional[License] = None

    access_notes: Optional[str] = None

    is_public: Optional[bool] = True

    outs: Any = None

    @property
    def path(self) -> Path:
        """Path to metadata file."""
        return Path(f"{paths.SNAPSHOTS_DIR / self.uri}.dvc")

    def to_yaml(self) -> str:
        """Convert to YAML string."""
        d = self.to_dict()

        # exclude `outs` with md5, we reset it when saving new metadata
        d.pop("outs", None)

        # remove is_public if it's True
        if d["is_public"]:
            del d["is_public"]

        # remove namespace/version/short_name/file_extension if they match path
        if _parse_snapshot_path(self.path) == (
            d["namespace"],
            str(d["version"]),
            d["short_name"],
            d["file_extension"],
        ):
            del d["namespace"]
            del d["version"]
            del d["short_name"]
            del d["file_extension"]

        return yaml_dump({"meta": d})  # type: ignore

    def save(self) -> None:
        self.path.parent.mkdir(exist_ok=True, parents=True)
        with open(self.path, "w") as f:
            f.write(self.to_yaml())

    @property
    def uri(self):
        return f"{self.namespace}/{self.version}/{self.short_name}.{self.file_extension}"

    @classmethod
    def load_from_yaml(cls, filename: Union[str, Path]) -> "SnapshotMeta":
        """Load metadata from YAML file. Metadata must be stored under `meta` key."""
        with open(filename) as istream:
            yml = yaml.safe_load(istream)
            if "meta" not in yml:
                raise ValueError("Metadata YAML should be stored under `meta` key")
            meta = yml["meta"]

            # fill metadata that can be inferred from path
            if "namespace" not in meta:
                meta["namespace"] = _parse_snapshot_path(Path(filename))[0]
            if "version" not in meta:
                meta["version"] = _parse_snapshot_path(Path(filename))[1]
            if "short_name" not in meta:
                meta["short_name"] = _parse_snapshot_path(Path(filename))[2]
            if "file_extension" not in meta:
                meta["file_extension"] = _parse_snapshot_path(Path(filename))[3]

            # convert legacy fields to source
            if "source" not in meta:
                publication_date = meta.pop("publication_date", None)
                meta["source"] = Source(
                    name=meta.pop("source_name", None),
                    description=meta.get("description", None),
                    published_by=meta.pop("source_published_by", None),
                    source_data_url=meta.pop("source_data_url", None),
                    url=meta.pop("url", None),
                    date_accessed=meta.pop("date_accessed", None),
                    publication_date=str(publication_date) if publication_date else None,
                    publication_year=meta.pop("publication_year", None),
                )

            if "license" not in meta:
                if "license_name" in meta or "license_url" in meta:
                    meta["license"] = License(
                        name=meta.pop("license_name", None),
                        url=meta.pop("license_url", None),
                    )

            return cls.from_dict(dict(**meta, outs=yml.get("outs", [])))

    @property
    def md5(self) -> str:
        if not self.outs:
            raise ValueError(f"Snapshot {self.uri} hasn't been added to DVC yet")
        assert len(self.outs) == 1
        return self.outs[0]["md5"]

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SnapshotMeta":
        ...

    def fill_from_backport_snapshot(self, snap_config_path: Path) -> None:
        """Load metadat from backported snapshot.

        Usage:
            snap_config = Snapshot(
                "backport/latest/dataset_3222_wheat_prices__long_run__in_england__makridakis_et_al__1997_config.json"
            )
            snap_config.pull()
            meta.fill_from_backport_snapshot(snap_config.path)
        """
        with open(snap_config_path) as f:
            js = json.load(f)

        # NOTE: this is similar to `convert_grapher_source`, DRY it when possible
        assert len(js["sources"]) == 1
        s = js["sources"][0]
        self.name = js["dataset"]["name"]
        self.source = Source(
            name=s["name"],
            description=s["description"].get("additionalInfo"),
            url=s["description"].get("link"),
            published_by=s["description"].get("dataPublishedBy"),
            date_accessed=pd.to_datetime(s["description"].get("retrievedDate") or dt.date.today()).date(),
        )


def add_snapshot(
    uri: str,
    filename: Optional[Union[str, Path]] = None,
    dataframe: Optional[pd.DataFrame] = None,
    upload: bool = False,
) -> None:
    """Helper function for adding snapshots with metadata, where the data is either
    a local file, or a dataframe in memory.

    Args:
        uri (str): URI of the snapshot file, typically `namespace/version/short_name.ext`. Metadata file
            `namespace/version/short_name.ext.dvc` must exist!
        filename (str or None): Path to local data file (if dataframe is not given).
        dataframe (pd.DataFrame or None): Dataframe to upload (if filename is not given).
        upload (bool): True to upload data to Walden bucket.
    """
    snap = Snapshot(uri)

    if (filename is not None) and (dataframe is None):
        # copy file to correct location
        shutil.copyfile(filename, snap.path)
    elif (dataframe is not None) and (filename is None):
        dataframes.to_file(dataframe, file_path=snap.path)
    else:
        raise ValueError("Use either 'filename' or 'dataframe' argument, but not both.")

    snap.dvc_add(upload=upload)


def snapshot_catalog(match: str = r".*") -> List[Snapshot]:
    """Return a catalog of all snapshots. It can take more than 10s to load the entire catalog,
    so it's recommended to use `match` to filter the snapshots.
    :param match: pattern to match uri
    """
    catalog = []
    for path in paths.SNAPSHOTS_DIR.glob("**/*.dvc"):
        uri = str(path.relative_to(paths.SNAPSHOTS_DIR)).replace(".dvc", "")
        if re.search(match, uri):
            catalog.append(Snapshot(uri))
    return catalog


@contextmanager
def _unignore_backports(path: Path):
    """Folder snapshots/backports contains thousands of .dvc files which adds significant overhead
    to running DVC commands (+8s overhead). That is why we ignore this folder in .dvcignore. This
    context manager checks if the path is in snapshots/backports and if so, temporarily removes
    this folder from .dvcignore.
    This makes non-backport DVC operations run under 1s and backport DVC operations at ~8s.
    Changing .dvcignore in-place is not great, but no other way was working (tried monkey-patching
    DVC and subrepos).
    """
    if "backport/" in str(path):
        with unignore_backports_lock:
            with open(".dvcignore") as f:
                s = f.read()
            try:
                with open(".dvcignore", "w") as f:
                    f.write(s.replace("snapshots/backport/", "# snapshots/backport/"))
                yield
            finally:
                with open(".dvcignore", "w") as f:
                    f.write(s)
    else:
        yield


def _parse_snapshot_path(path: Path) -> tuple[str, str, str, str]:
    """Parse snapshot path into namespace, short_name, file_extension."""
    version = path.parent.name
    namespace = path.parent.parent.name

    short_name, ext = path.stem.split(".", 1)
    assert "." not in ext, f"{path.name} cannot contain `.`"
    return namespace, version, short_name, ext
