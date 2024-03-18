import datetime as dt
import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

import owid.catalog.processing as pr
import pandas as pd
import structlog
import yaml
from dataclasses_json import dataclass_json
from owid.catalog import Table, s3_utils
from owid.catalog.meta import (
    DatasetMeta,
    License,
    Origin,
    Source,
    TableMeta,
    pruned_json,
)
from owid.datautils import dataframes
from owid.datautils.io import decompress_file
from owid.walden import files

from etl import config, paths
from etl.files import checksum_file, ruamel_dump, ruamel_load, yaml_dump

log = structlog.get_logger()


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
            raise FileNotFoundError(f"Metadata file {self.metadata_path} not found, but {uri} is in DAG.")

        self.metadata = SnapshotMeta.load_from_yaml(self.metadata_path)

    @property
    def m(self) -> "SnapshotMeta":
        """Metadata alias to save typing."""
        return self.metadata

    @property
    def path(self) -> Path:
        """Path to materialized file."""
        return paths.DATA_DIR / "snapshots" / self.uri

    @property
    def metadata_path(self) -> Path:
        """Path to metadata file."""
        archive_path = Path(f"{paths.SNAPSHOTS_DIR_ARCHIVE / self.uri}.dvc")
        if archive_path.exists():
            return archive_path
        else:
            return Path(f"{paths.SNAPSHOTS_DIR / self.uri}.dvc")

    def _download_dvc_file(self, md5: str) -> None:
        """Download file from remote to self.path."""
        self.path.parent.mkdir(exist_ok=True, parents=True)
        if self.metadata.is_public:
            # TODO: temporarily download files from R2 instead of public link to prevent
            # issues with cached snapshots. Remove this when convenient
            download_url = f"{config.R2_SNAPSHOTS_PUBLIC_READ}/{md5[:2]}/{md5[2:]}"
            files.download(download_url, str(self.path), progress_bar_min_bytes=2**100)
        else:
            download_url = f"s3://{config.R2_SNAPSHOTS_PRIVATE}/{md5[:2]}/{md5[2:]}"
            s3_utils.download(download_url, str(self.path))

        # Check if file was downloaded correctly. This should never happen
        downloaded_md5 = checksum_file(self.path)
        if downloaded_md5 != md5:
            # remove the downloaded file
            self.path.unlink()
            raise ValueError(f"Checksum mismatch for {self.path}: expected {md5}, got {downloaded_md5}")

    def pull(self, force=True) -> None:
        """Pull file from S3."""
        if not force and not self.is_dirty():
            return

        assert len(self.metadata.outs) == 1
        md5 = self.metadata.outs[0]["md5"]

        self._download_dvc_file(md5)

        expected_size = self.metadata.outs[0]["size"]
        downloaded_size = self.path.stat().st_size
        if downloaded_size != expected_size:
            # remove the downloaded file
            self.path.unlink()
            raise ValueError(f"Size mismatch for {self.path}: expected {expected_size}, got {downloaded_size}")

    def is_dirty(self) -> bool:
        """Return True if snapshot exists and is in DVC."""
        if not self.path.exists():
            return True

        if self.metadata.outs is None:
            raise Exception(f"File {self.metadata_path} has not been added to DVC. Run snapshot script to add it.")

        assert len(self.metadata.outs) == 1
        file_size = self.path.stat().st_size
        # Compare file size if it's larger than 10MB, otherwise compare md5
        # This should be pretty safe and speeds up the process significantly
        if file_size >= 10 * 2**20:  # 10MB
            return file_size != self.m.outs[0]["size"]
        else:
            return checksum_file(self.path.as_posix()) != self.m.outs[0]["md5"]

    def delete_local(self) -> None:
        """Delete local file and its metadata."""
        if self.path.exists():
            self.path.unlink()
        if self.metadata_path.exists():
            self.metadata_path.unlink()

    def download_from_source(self) -> None:
        """Download file from source_data_url."""
        if self.metadata.origin:
            assert self.metadata.origin.url_download, "url_download is not set"
            download_url = self.metadata.origin.url_download
        elif self.metadata.source:
            assert self.metadata.source.source_data_url, "source_data_url is not set"
            download_url = self.metadata.source.source_data_url
        else:
            raise ValueError("Neither origin nor source is set")
        self.path.parent.mkdir(exist_ok=True, parents=True)
        if download_url.startswith("s3://") or download_url.startswith("r2://"):
            s3_utils.download(download_url, str(self.path))
        else:
            files.download(download_url, str(self.path))

    def dvc_add(self, upload: bool) -> None:
        """Add file to DVC and upload to S3."""
        if not upload:
            log.warn("Skipping upload", snapshot=self.uri)
            return

        # Upload to S3
        md5 = checksum_file(self.path)
        bucket = config.R2_SNAPSHOTS_PUBLIC if self.metadata.is_public else config.R2_SNAPSHOTS_PRIVATE
        s3_utils.upload(f"s3://{bucket}/{md5[:2]}/{md5[2:]}", str(self.path), public=self.metadata.is_public)

        # Update metadata file
        with open(self.metadata_path, "r") as f:
            meta = ruamel_load(f)

        meta["outs"] = [{"md5": md5, "size": self.path.stat().st_size, "path": self.path.name}]

        with open(self.metadata_path, "w") as f:
            f.write(ruamel_dump(meta))

    def create_snapshot(
        self,
        filename: Optional[Union[str, Path]] = None,
        data: Optional[Union[Table, pd.DataFrame]] = None,
        upload: bool = False,
    ) -> None:
        """Create a new snapshot from a local file, or from data in memory, or from a download link."""
        if (filename is not None) or (data is not None):
            # Create snapshot from either a local file or from data in memory.
            add_snapshot(uri=self.uri, filename=filename, dataframe=data, upload=upload)
        else:
            # Create snapshot by downloading data from a URL.
            self.download_from_source()
            self.dvc_add(upload=upload)

    def to_table_metadata(self):
        return self.metadata.to_table_metadata()

    def read(self, *args, **kwargs) -> Table:
        """Read file based on its Snapshot extension."""
        if self.metadata.file_extension == "csv":
            return self.read_csv(*args, **kwargs)
        elif self.metadata.file_extension == "feather":
            return self.read_feather(*args, **kwargs)
        elif self.metadata.file_extension in ["xlsx", "xls", "xlsm", "xlsb", "odf", "ods", "odt"]:
            return self.read_excel(*args, **kwargs)
        elif self.metadata.file_extension == "json":
            return self.read_json(*args, **kwargs)
        elif self.metadata.file_extension == "dta":
            return self.read_stata(*args, **kwargs)
        elif self.metadata.file_extension == "rds":
            return self.read_rds(*args, **kwargs)
        elif self.metadata.file_extension == "rda":
            return self.read_rda(*args, **kwargs)
        else:
            raise ValueError(f"Unknown extension {self.metadata.file_extension}")

    def read_csv(self, *args, **kwargs) -> Table:
        """Read CSV file into a Table and populate it with metadata."""
        return pr.read_csv(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_feather(self, *args, **kwargs) -> Table:
        """Read feather file into a Table and populate it with metadata."""
        return pr.read_feather(
            self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs
        )

    def read_excel(self, *args, **kwargs) -> Table:
        """Read excel file into a Table and populate it with metadata."""
        return pr.read_excel(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_json(self, *args, **kwargs) -> Table:
        """Read JSON file into a Table and populate it with metadata."""
        return pr.read_json(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_stata(self, *args, **kwargs) -> Table:
        """Read Stata file into a Table and populate it with metadata."""
        return pr.read_stata(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_rds(self, *args, **kwargs) -> Table:
        """Read R data .rds file into a Table and populate it with metadata."""
        return pr.read_rds(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_rda(self, *args, **kwargs) -> Table:
        """Read R data .rda file into a Table and populate it with metadata."""
        return pr.read_rda(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_from_records(self, *args, **kwargs) -> Table:
        """Read records into a Table and populate it with metadata."""
        return pr.read_from_records(*args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_from_dict(self, *args, **kwargs) -> Table:
        """Read data from a dictionary into a Table and populate it with metadata."""
        return pr.read_from_dict(*args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_fwf(self, *args, **kwargs) -> Table:
        """Read a table of fixed-width formatted lines with metadata."""
        return pr.read_fwf(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def ExcelFile(self, *args, **kwargs) -> pr.ExcelFile:
        """Return an Excel file object ready for parsing."""
        return pr.ExcelFile(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def extract(self, output_dir: Path | str):
        decompress_file(self.path, output_dir)

    def extract_to_tempdir(self) -> Any:
        # Create temporary directory
        temp_dir = tempfile.TemporaryDirectory()
        # Extract file to temporary directory
        self.extract(temp_dir.name)
        # Return temporary directory
        return temp_dir

    def read_in_zip(self, filename: str, *args, **kwargs) -> Table:
        """Read data from file inside a zip archive.

        If the relevant data file is within a zip archive, this method will read this file and return it as a table.

        To do so, this method first unzips the archive to a temporary directory, and then reads the file. Note that the file should have a supported extension (see `read` method).
        """
        with self.extract_to_tempdir() as tmpdir:
            # Temporarily change the file extension so that .read works
            extension_original = self.metadata.file_extension
            self.metadata.file_extension = filename.split(".")[-1]
            # Read
            tb = self.read(Path(tmpdir) / filename, *args, **kwargs)
            # Restore original file extension
            self.metadata.file_extension = extension_original
            return tb


@pruned_json
@dataclass_json
@dataclass
class SnapshotMeta:
    # how we identify the dataset, determined automatically from snapshot path
    namespace: str  # a short source name (usually institution name)
    version: str  # date, `latest` or year (discouraged)
    short_name: str  # a slug, ideally unique, snake_case, no spaces
    file_extension: str

    # NOTE: origin should actually never be None, it's here for backward compatibility
    origin: Optional[Origin] = None
    source: Optional[Source] = None  # source is being slowly deprecated, use origin instead

    # name and description are usually part of origin or source, they are here only for backward compatibility
    name: Optional[str] = None
    description: Optional[str] = None

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

        # remove default values
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

            if "origin" in meta:
                meta["origin"] = Origin.from_dict(meta["origin"])

            if "source" in meta:
                meta["source"] = Source.from_dict(meta["source"])
            elif "source_name" in meta:
                # convert legacy fields to source
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
            assert meta.get("origin") or meta.get("source"), 'Either "origin" or "source" must be set'

            if "license" not in meta:
                if "license_name" in meta or "license_url" in meta:
                    meta["license"] = License(
                        name=meta.pop("license_name", None),
                        url=meta.pop("license_url", None),
                    )

            snap_meta = cls.from_dict(dict(**meta, outs=yml.get("outs", [])))

            return snap_meta

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
            date_accessed=pd.to_datetime(
                s["description"].get("retrievedDate") or dt.date.today(), dayfirst=True
            ).date(),
        )

    def to_table_metadata(self):
        if "origin" in self.to_dict():
            table_meta = TableMeta.from_dict(
                {
                    "short_name": self.short_name,
                    "title": self.origin.title,  # type: ignore
                    "description": self.origin.description,  # type: ignore
                    "dataset": DatasetMeta.from_dict(
                        {
                            "channel": "snapshots",
                            "namespace": self.namespace,
                            "short_name": self.short_name,
                            "title": self.origin.title,  # type: ignore
                            "description": self.origin.description,  # type: ignore
                            "licenses": [self.license] if self.license else [],
                            "is_public": self.is_public,
                            "version": self.version,
                        }
                    ),
                }
            )
        else:
            table_meta = TableMeta.from_dict(
                {
                    "short_name": self.short_name,
                    "title": self.name,
                    "description": self.description,
                    "dataset": DatasetMeta.from_dict(
                        {
                            "channel": "snapshots",
                            "description": self.description,
                            "is_public": self.is_public,
                            "namespace": self.namespace,
                            "short_name": self.short_name,
                            "title": self.name,
                            "version": self.version,
                            "sources": [self.source] if self.source else [],
                            "licenses": [self.license] if self.license else [],
                        }
                    ),
                }
            )
        return table_meta


def add_snapshot(
    uri: str,
    filename: Optional[Union[str, Path]] = None,
    dataframe: Optional[Union[Table, pd.DataFrame]] = None,
    upload: bool = False,
) -> None:
    """Helper function for adding snapshots with metadata, where the data is either
    a local file, or a dataframe in memory.

    Args:
        uri (str): URI of the snapshot file, typically `namespace/version/short_name.ext`. Metadata file
            `namespace/version/short_name.ext.dvc` must exist!
        filename (str or None): Path to local data file (if dataframe is not given).
        dataframe (Table or pd.DataFrame or None): Data to upload (if filename is not given).
        upload (bool): True to upload data to Walden bucket.
    """
    snap = Snapshot(uri)

    if (filename is not None) and (dataframe is None):
        # Ensure destination folder exists.
        snap.path.parent.mkdir(exist_ok=True, parents=True)

        # Copy local data file to snapshots data folder.
        snap.path.write_bytes(Path(filename).read_bytes())
    elif (dataframe is not None) and (filename is None):
        dataframes.to_file(dataframe, file_path=snap.path)
    else:
        raise ValueError("Pass either a filename or data, but not both.")

    snap.dvc_add(upload=upload)


def snapshot_catalog(match: str = r".*") -> Iterator[Snapshot]:
    """Return a catalog of all snapshots. It can take more than 10s to load the entire catalog,
    so it's recommended to use `match` to filter the snapshots.
    :param match: pattern to match uri
    """
    for path in paths.SNAPSHOTS_DIR.glob("**/*.dvc"):
        uri = str(path.relative_to(paths.SNAPSHOTS_DIR)).replace(".dvc", "")
        if re.search(match, uri):
            yield Snapshot(uri)


def _parse_snapshot_path(path: Path) -> tuple[str, str, str, str]:
    """Parse snapshot path into namespace, short_name, file_extension."""
    version = path.parent.name
    namespace = path.parent.parent.name

    short_name, ext = path.stem.split(".", 1)
    assert "." not in ext, f"{path.name} cannot contain `.`"
    return namespace, version, short_name, ext
