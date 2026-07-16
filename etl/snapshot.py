import re
import tempfile
import time
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import owid.catalog.core.processing as pr
import pandas as pd
import requests
import structlog
import yaml
from deprecated import deprecated
from owid.catalog import Table, s3_utils
from owid.catalog.core.meta import (
    DatasetMeta,
    License,
    MetaBase,
    Origin,
    TableMeta,
    pruned_json,
)
from owid.datautils import dataframes
from owid.datautils.io import decompress_file
from owid.repack import to_safe_types
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from etl import config, download_helpers, paths
from etl.download_helpers import DownloadCorrupted
from etl.files import checksum_file, ruamel_dump, ruamel_load, yaml_dump, yaml_load

log = structlog.get_logger()


class SnapshotNotFoundException(Exception):
    """Raised when a snapshot file is not found on the remote server.

    This is a plain Exception subclass (no unpicklable attributes) so it can
    safely travel across process boundaries in ProcessPoolExecutor workers.
    """

    def __init__(self, uri: str, md5: str) -> None:
        super().__init__(
            f"Snapshot file not found on the remote server: {uri} (md5: {md5}). "
            f"Have you run `etls {uri} --upload` to upload it?"
        )


class SnapshotArchive:
    """Context manager for reading files from snapshot archives.

    Provides an intuitive interface for working with archived snapshot files:

    Example:
        ```python
        with snap.extracted() as archive:
            # List all files
            print(archive.files)  # ['data/file1.csv', 'data/file2.csv']

            # Find files with glob patterns
            csv_files = archive.glob("**/*.csv")

            # Read a file (with helpful error if not found)
            tb = archive.read("data/file1.csv")

            # Check if file exists
            if "data/file1.csv" in archive:
                ...

            # Access path for custom operations
            path = archive.path / "data" / "file1.csv"
        ```
    """

    def __init__(self, snapshot: "Snapshot", path: Path):  # noqa: F821
        self._snapshot = snapshot
        self._path = path
        self._files: list[str] | None = None

    @property
    def path(self) -> Path:
        """Root path of extracted archive."""
        return self._path

    @property
    def files(self) -> list[str]:
        """List all files in the archive (relative paths, sorted)."""
        if self._files is None:
            self._files = sorted(str(p.relative_to(self._path)) for p in self._path.rglob("*") if p.is_file())
        return self._files

    def glob(self, pattern: str) -> list[str]:
        """Find files matching a glob pattern.

        Args:
            pattern: Glob pattern to match files against.

        Returns:
            List of matching file paths (relative to archive root), sorted.

        Examples:
            ```python
            archive.glob("*.csv")         # CSVs in root
            archive.glob("**/*.csv")      # All CSVs recursively
            archive.glob("data/*.csv")    # CSVs in data/ folder
            ```
        """
        return sorted(str(p.relative_to(self._path)) for p in self._path.glob(pattern) if p.is_file())

    def __contains__(self, filename: str) -> bool:
        """Check if file exists in archive.

        Args:
            filename: Relative path to check.

        Returns:
            True if file exists in archive.

        Example:
            ```python
            if "data.csv" in archive:
                tb = archive.read("data.csv")
            ```
        """
        return (self._path / filename).is_file()

    def read(self, filename: str, force_extension: str | None = None, **kwargs) -> Table:
        """Read a file from the archive.

        Args:
            filename: Relative path to the file within the archive.
            force_extension: Override file extension for read method selection.
            **kwargs: Additional arguments passed to the read function.

        Returns:
            Table with the file contents and snapshot metadata.

        Raises:
            FileNotFoundError: If file doesn't exist, with helpful message listing available files.

        Example:
            ```python
            with snap.extracted() as archive:
                tb = archive.read("data/2020.csv")
            ```
        """
        file_path = self._path / filename
        if not file_path.is_file():
            available = "\n".join(f"  - {f}" for f in self.files)
            raise FileNotFoundError(f"File '{filename}' not found in archive.\nAvailable files:\n{available}")

        if force_extension is None:
            extension = filename.split(".")[-1]
        else:
            extension = force_extension

        return read_table_from_snapshot(
            path=file_path,
            table_metadata=self._snapshot.to_table_metadata(),
            snapshot_origin=self._snapshot.metadata.origin,
            file_extension=extension,
            **kwargs,
        )


@dataclass
class Snapshot:
    uri: str
    metadata: "SnapshotMeta"
    _unarchived_dir: Path | None = None

    def __init__(self, uri: str) -> None:
        """
        :param uri: URI of the snapshot file, typically `namespace/version/short_name.ext`
        """
        self.uri = uri

        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Metadata file {self.metadata_path} not found, but {uri} is in DAG.")

        self.metadata = SnapshotMeta.load_from_yaml(self.metadata_path)

    @classmethod
    def from_raw_uri(cls, raw_uri: str) -> "Snapshot":
        """Create Snapshot from raw URI."""
        if raw_uri.startswith("snapshot://"):
            snap_uri = raw_uri.replace("snapshot://", "")
        elif raw_uri.startswith("snapshot-private://"):
            snap_uri = raw_uri.replace("snapshot-private://", "")
        else:
            raise ValueError(f"Invalid URI: {raw_uri}")
        return cls(snap_uri)

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
        return Path(f"{paths.SNAPSHOTS_DIR / self.uri}.dvc")

    def _snapshot_exists_on_remote(self, md5: str) -> bool:
        """Check if snapshot file exists on R2 without downloading it."""
        if self.metadata.is_public:
            url = f"{config.R2_SNAPSHOTS_PUBLIC_READ}/{md5[:2]}/{md5[2:]}"
            try:
                resp = requests.head(url, timeout=10)
                return resp.status_code == 200
            except requests.RequestException:
                return False
        else:
            # For private snapshots, assume it exists if md5 matches — we can't
            # easily do a HEAD request on S3 without more setup
            return True

    def _download_dvc_file(self, md5: str) -> None:
        """Download file from remote to self.path."""
        self.path.parent.mkdir(exist_ok=True, parents=True)
        if self.metadata.is_public:
            # TODO: temporarily download files from R2 instead of public link to prevent
            # issues with cached snapshots. Remove this when convenient
            download_url = f"{config.R2_SNAPSHOTS_PUBLIC_READ}/{md5[:2]}/{md5[2:]}"
            try:
                download_helpers.download(download_url, str(self.path), progress_bar_min_bytes=2**100)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    raise SnapshotNotFoundException(self.uri, md5) from None
                raise
        else:
            download_url = f"s3://{config.R2_SNAPSHOTS_PRIVATE}/{md5[:2]}/{md5[2:]}"
            s3_utils.download(download_url, str(self.path))

        # Check if file was downloaded correctly. This should never happen
        downloaded_md5 = checksum_file(self.path)
        if downloaded_md5 != md5:
            # remove the downloaded file
            self.path.unlink()
            raise ValueError(
                f"Checksum mismatch for {self.path}: expected {md5}, got {downloaded_md5}. It is possible that download got interrupted."
            )

    def pull(self, force=True, retries: int = 1) -> None:
        """Pull file from S3."""
        if not force and not self.is_dirty():
            return

        assert len(self.metadata.outs) == 1, ".dvc file is missing 'outs' field. Have you run the snapshot?"
        expected_md5 = self.metadata.outs[0]["md5"]

        if retries > 1:
            for attempt in Retrying(
                retry=retry_if_exception_type(
                    (requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError, DownloadCorrupted)
                ),
                stop=stop_after_attempt(retries),
                wait=wait_exponential(multiplier=1, min=1, max=10),
            ):
                with attempt:
                    self._download_dvc_file(expected_md5)
        else:
            self._download_dvc_file(expected_md5)

        expected_size = self.metadata.outs[0]["size"]
        downloaded_size = self.path.stat().st_size
        if downloaded_size != expected_size:
            # remove the downloaded file
            self.path.unlink()
            raise ValueError(f"Size mismatch for {self.path}: expected {expected_size}, got {downloaded_size}")

        downloaded_md5 = checksum_file(self.path)
        if downloaded_md5 != expected_md5:
            # remove the downloaded file
            self.path.unlink()
            raise ValueError(f"Checksum mismatch for {self.path}: expected {expected_md5}, got {downloaded_md5}")

    def is_dirty(self) -> bool:
        """Return True if snapshot exists and is in DVC."""
        if not self.path.exists():
            return True

        if self.metadata.outs is None:
            raise Exception(f"File {self.metadata_path} has not been added to DVC. Run snapshot script to add it.")

        assert len(self.metadata.outs) == 1, ".dvc file is missing 'outs' field. Have you run the snapshot?"
        file_size = self.path.stat().st_size
        # Compare file size if it's larger than 20MB, otherwise compare md5
        # This should be pretty safe and speeds up the process significantly
        # NOTE: on 2024-06-12 this caused a discrepancy between production and staging
        # for snapshot://climate/latest/weekly_wildfires.csv.dvc. Data was slightly updated, but
        # the file size was the same. This should be a very rare case.
        if file_size >= 20 * 2**20:  # 20MB
            return file_size != self.m.outs[0]["size"]
        else:
            return checksum_file(self.path.as_posix()) != self.m.outs[0]["md5"]

    def delete_local(self) -> None:
        """Delete local file and its metadata."""
        if self.path.exists():
            self.path.unlink()
        if self.metadata_path.exists():
            self.metadata_path.unlink()

    def download_from_source(self, user_agent: str | None = None) -> None:
        """Download file from origin.url_download.

        :param user_agent: Optional User-Agent header to use for HTTP(S) downloads (ignored for
            s3://r2:// sources). See `download_helpers.download` for the default and bot-wall fallback.
        """
        assert self.metadata.origin, "origin is not set"
        assert self.metadata.origin.url_download, "url_download is not set"
        download_url = self.metadata.origin.url_download
        self.path.parent.mkdir(exist_ok=True, parents=True)
        if download_url.startswith("s3://") or download_url.startswith("r2://"):
            s3_utils.download(download_url, str(self.path))
        else:
            download_helpers.download(download_url, str(self.path), user_agent=user_agent)

    def dvc_add(self, upload: bool) -> None:
        """Add a file to DVC and upload it to S3.

        This method only handles uploading the file. Ensure that the file is in the correct location,
        usually by calling:

        ```
        snap.download_from_source()
        snap.dvc_add(upload=upload)
        ```

        It is recommended to use `snap.create_snapshot`, which handles all of these steps.
        """
        if not upload:
            log.warn("Skipping upload", snapshot=self.uri)
            return
        # Calculate md5
        md5 = checksum_file(self.path)

        # Get metadata file
        with open(self.metadata_path) as f:
            meta = ruamel_load(f)

        # If the file already exists with the same md5, verify it's actually on R2 before skipping
        if meta.get("outs") and meta["outs"][0]["md5"] == md5:
            if self._snapshot_exists_on_remote(md5):
                log.info("File already exists with the same md5, skipping upload", snapshot=self.uri)
                return
            else:
                log.warning("File md5 matches .dvc metadata but is missing from R2, re-uploading", snapshot=self.uri)

        # Upload to S3
        bucket = config.R2_SNAPSHOTS_PUBLIC if self.metadata.is_public else config.R2_SNAPSHOTS_PRIVATE
        assert self.metadata.is_public is not None
        s3_utils.upload(f"s3://{bucket}/{md5[:2]}/{md5[2:]}", str(self.path), public=self.metadata.is_public)

        self.m._update_metadata_file({"outs": [{"md5": md5, "size": self.path.stat().st_size, "path": self.path.name}]})

    def create_snapshot(
        self,
        filename: str | Path | None = None,
        data: Table | pd.DataFrame | None = None,
        upload: bool = False,
        download_retries: int = 1,
        user_agent: str | None = None,
    ) -> None:
        """Create a new snapshot from a local file, or from data in memory, or from a download link.
        Then upload it to S3. This is the recommended way to create a snapshot.

        Args:
            uri (str): URI of the snapshot file, typically `namespace/version/short_name.ext`. Metadata file
                `namespace/version/short_name.ext.dvc` must exist!
            filename (str or None): Path to local data file (if dataframe is not given).
            data (Table or pd.DataFrame or None): Data to upload (if filename is not given).
            upload (bool): True to upload data to bucket.
            download_retries (int): Number of retries for downloading from source (default: 1, no retries).
            user_agent (str or None): User-Agent to use when downloading from `url_download` (only relevant
                for HTTP(S) sources). Defaults to a browser-like UA with a non-browser fallback for bot-walls.
        """
        assert not (filename is not None and data is not None), "Pass either a filename or data, but not both."

        if filename is not None:
            # Ensure destination folder exists.
            self.path.parent.mkdir(exist_ok=True, parents=True)

            # Copy local data file to snapshots data folder.
            self.path.write_bytes(Path(filename).read_bytes())
        elif data is not None:
            # Copy dataframe to snapshots data folder.
            dataframes.to_file(data, file_path=self.path)
        elif self.metadata.origin and self.metadata.origin.url_download:
            # Create snapshot by downloading data from a URL with retry logic.
            for attempt in range(1, download_retries + 1):
                try:
                    self.download_from_source(user_agent=user_agent)
                    break
                except DownloadCorrupted as e:
                    log.warning(
                        str(e),
                        attempt=attempt,
                        max_attempts=download_retries,
                    )
                    if attempt == download_retries:
                        # Re-raise the exception on final attempt
                        raise
                    else:
                        # Wait before retrying (exponential backoff)
                        wait_time = min(4 * (2 ** (attempt - 1)), 10)
                        time.sleep(wait_time)
        else:
            # Maybe file is already there
            assert self.path.exists(), "File not found. Provide a filename, data or add url_download to metadata."

        # Upload data to R2
        self.dvc_add(upload=upload)

        # Save metadata to file
        self.metadata.save()

    def to_table_metadata(self) -> TableMeta:
        return self.metadata.to_table_metadata()

    def read(self, file_extension: str | None = None, *args, **kwargs) -> Table:
        """Read file based on its Snapshot extension."""
        return read_table_from_snapshot(
            *args,
            path=self.path,
            table_metadata=self.to_table_metadata(),
            snapshot_origin=self.metadata.origin,
            file_extension=file_extension if file_extension is not None else self.metadata.file_extension,
            **kwargs,
        )

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

    def read_rda_multiple(self, *args, **kwargs) -> dict[str, Table]:
        """Read R data .rda file into multiple Tables and populate it with metadata.

        RData objects can contain multiple dataframes.

        Read specific dataframes from an RData file:

        ```python
        tables = snap.read_rda_multiple(["tname1", "tname2"])
        ```

        If you don't provide any table names, all tables will be read:

        ```python
        tables = snap.read_rda_multiple()
        ```

        where tables is a key-value dictionary, and keys are the names of the tables (same as table short_names too).
        """
        return pr.read_rda_multiple(
            self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs
        )

    def read_fwf(self, *args, **kwargs) -> Table:
        """Read a table of fixed-width formatted lines with metadata."""
        return pr.read_fwf(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_from_records(self, *args, **kwargs) -> Table:
        """Read records into a Table and populate it with metadata."""
        return pr.read_from_records(*args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_from_dict(self, *args, **kwargs) -> Table:
        """Read data from a dictionary into a Table and populate it with metadata."""
        return pr.read_from_dict(*args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def ExcelFile(self, *args, **kwargs) -> pr.ExcelFile:
        """Return an Excel file object ready for parsing."""
        return pr.ExcelFile(self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_parquet(self, *args, **kwargs) -> Table:
        """Read parquet file into a Table and populate it with metadata."""
        return pr.read_parquet(
            self.path, *args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs
        )

    def read_from_df(self, *args, **kwargs) -> Table:
        """Read data from a dataframe into a Table and populate it with metadata."""
        return pr.read_from_df(*args, metadata=self.to_table_metadata(), origin=self.metadata.origin, **kwargs)

    def read_custom(self, read_function: Callable, *args, **kwargs) -> Table:
        """Read data file using a custom reader function, and return a Table with metadata.

        Use this method when standard read methods (read_csv, read_excel, etc.) don't meet
        your needs. The custom function receives the snapshot file path and should return
        a pandas DataFrame or compatible data structure.

        Parameters
        ----------
        read_function : Callable
            Custom function to read the data. Must accept a file path as first argument
            and return a DataFrame or Table.
        *args
            Additional positional arguments to pass to read_function.
        **kwargs
            Additional keyword arguments to pass to read_function.

        Returns
        -------
        Table
            Data read by the custom function as a Table with snapshot metadata.

        Examples
        --------
        Read a table from an HTML file:

        ```python
        tb = snap.read_custom(read_function=lambda x: pd.read_html(x)[0])
        ```
        """
        return pr.read_custom(
            filepath_or_buffer=self.path,
            read_function=read_function,
            *args,
            metadata=self.to_table_metadata(),
            origin=self.metadata.origin,
            **kwargs,
        )

    @contextmanager
    def extracted(self) -> Generator[SnapshotArchive, None, None]:
        """Extract archive to temporary directory and provide access to its contents.

        Returns a SnapshotArchive object that provides an intuitive interface
        for listing and reading files from the archive. The temporary directory
        is automatically cleaned up when the context manager exits.

        Yields:
            SnapshotArchive: Object with methods for listing and reading archive contents.

        Example:
            ```python
            snap = Snapshot(...)

            with snap.extracted() as archive:
                # List all files
                print(archive.files)  # ['data/file1.csv', 'meta/info.json']

                # Find files with glob patterns
                csv_files = archive.glob("**/*.csv")

                # Read a file
                tb = archive.read("data/file1.csv")

                # Check if file exists
                if "optional.csv" in archive:
                    ...
            ```
        """
        temp_dir = tempfile.TemporaryDirectory()
        try:
            decompress_file(self.path, temp_dir.name)
            archive = SnapshotArchive(self, Path(temp_dir.name))
            # Keep backward compatibility
            self._unarchived_dir = archive.path
            yield archive
        finally:
            temp_dir.cleanup()
            self._unarchived_dir = None

    def read_from_archive(self, filename: str, force_extension: str | None = None, **kwargs) -> Table:
        """Read a file in an archive.

        Use this function within a 'with snap.extracted():' context manager. Otherwise it'll raise a RuntimeError, since `_unarchived_dir` will be None.

        The read method is inferred based on the file extension of `filename`. Use `force_extension` if you want to override this.

        Note:
            Consider using `archive.read()` directly from the context manager for better error messages:

            ```python
            with snap.extracted() as archive:
                tb = archive.read("filename.csv")  # Better error messages
            ```

        Example:
            ```python
            snap = Snapshot(...)

            with snap.extracted():
                table1 = snap.read_from_archive("filename1.csv")
                table2 = snap.read_from_archive("filename2.csv")
            ```
        """
        if not hasattr(self, "_unarchived_dir") or self._unarchived_dir is None:
            raise RuntimeError("Archive is not unarchived. Use 'with snap.extracted()' context manager.")

        # Delegate to SnapshotArchive.read() for consistent error handling
        archive = SnapshotArchive(self, self._unarchived_dir)
        return archive.read(filename, force_extension=force_extension, **kwargs)

    @property
    def path_unarchived(self) -> Path:
        if not hasattr(self, "_unarchived_dir") or self._unarchived_dir is None:
            raise RuntimeError("Archive is not unarchived. Use 'with snap.extracted():' context manager.")

        return self._unarchived_dir

    # Methods to deal with archived files
    @deprecated("This function will be deprecated. Use `extracted()` context manager instead.")
    def extract(self, output_dir: Path | str):
        decompress_file(self.path, output_dir)

    @deprecated("This function will be deprecated. Use `extracted()` context manager instead.")
    def extract_to_tempdir(self) -> Any:
        # Create temporary directory
        temp_dir = tempfile.TemporaryDirectory()
        # Extract file to temporary directory
        decompress_file(self.path, temp_dir.name)
        # Return temporary directory
        return temp_dir

    @deprecated("This function will be deprecated. Use `extracted()` context manager instead.")
    def read_in_archive(self, filename: str, force_extension: str | None = None, *args, **kwargs) -> Table:
        """Read data from file inside a zip/tar archive.

        DEPRECATED: This function will be deprecated. Use `extracted()` context manager instead.
            >>> with snap.extracted():
            ...     table1 = snap.read_from_archive("filename1.csv")
            ...     table2 = snap.read_from_archive("filename2.csv")

        If the relevant data file is within a zip/tar archive, this method will read this file and return it as a table.

        To do so, this method first unzips/untars the archive to a temporary directory, and then reads the file. Note that the file should have a supported extension (see `read` method).

        The read method is inferred based on the file extension of `filename`. Use `force_extension` if you want to override this.
        """
        with self.extract_to_tempdir() as tmpdir:
            if force_extension is None:
                new_extension = filename.split(".")[-1]
            else:
                new_extension = force_extension

            # Read
            tb = read_table_from_snapshot(
                *args,
                path=Path(tmpdir) / filename,
                table_metadata=self.to_table_metadata(),
                snapshot_origin=self.metadata.origin,
                file_extension=new_extension,
                **kwargs,
            )
            return tb


@pruned_json
@dataclass
class SnapshotMeta(MetaBase):
    # how we identify the dataset, determined automatically from snapshot path
    namespace: str  # a short source name (usually institution name)
    version: str  # date, `latest` or year (discouraged)
    short_name: str  # a slug, ideally unique, snake_case, no spaces
    file_extension: str

    # NOTE: origin should actually never be None, it's here for backward compatibility
    origin: Origin | None = None

    # name and description are usually part of origin, they are here only for backward compatibility
    name: str | None = None
    description: str | None = None

    # DEPRECATED top-level license. The license belongs under `origin.license` so it travels with
    # the origin (and reaches Grapher's per-origin metadata). This field only exists for backward
    # compatibility; new snapshots must set `meta.origin.license`, not `meta.license`. Enforced by
    # `tests/test_metadata_schemas.py::test_snapshot_license_lives_under_origin` and the `not`
    # constraint in `schemas/snapshot-schema.json`.
    license: License | None = None

    access_notes: str | None = None

    is_public: bool | None = True

    outs: Any = None

    @property
    def path(self) -> Path:
        """Path to metadata file."""
        return Path(f"{paths.SNAPSHOTS_DIR / self.uri}.dvc")

    def _meta_to_dict(self):
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

        return d

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

        return yaml_dump({"meta": d})  # ty: ignore

    def _update_metadata_file(self, d: dict[str, Any]) -> None:
        """Update metadata YAML file with given dictionary."""
        with open(self.path) as f:
            meta = ruamel_load(f)

        # Update everything from `meta`
        update_meta = d.pop("meta", {})
        for k, v in update_meta.items():
            if k in meta["meta"]:
                if isinstance(meta["meta"][k], dict):
                    meta["meta"][k].update(v)
                else:
                    meta["meta"][k] = v
            else:
                meta["meta"][k] = v

        # Update remaining fields
        meta.update(d)

        with open(self.path, "w") as f:
            f.write(ruamel_dump(meta))

    def save(self) -> None:  # ty: ignore
        """Save metadata to YAML file. This is useful if you're dynamically changing
        metadata (like dates) from the script and need to save them into YAML. This
        function doesn't upload the file to S3, use `create_snapshot` instead.
        """
        self.path.parent.mkdir(exist_ok=True, parents=True)

        # Create new file
        if not self.path.exists():
            with open(self.path, "w") as f:
                f.write(self.to_yaml())
        # Edit existing file, keep outs
        else:
            # Load outs from existing file
            with open(self.path) as f:
                yaml = yaml_load(f)
                outs = yaml.get("outs", None)
                # wdir is a legacy field, we just ignore it

            # Save metadata to file
            # NOTE: meta does not have `outs` field, it's reset when saving
            meta = self._meta_to_dict()

            # No change, keep the file as is
            if yaml["meta"] == meta:
                return

            # Otherwise update the file
            # set `outs` back
            d = {"meta": meta}
            if outs:
                d["outs"] = outs
            self._update_metadata_file(d)

    @property
    def uri(self):
        return f"{self.namespace}/{self.version}/{self.short_name}.{self.file_extension}"

    @classmethod
    def load_from_yaml(cls, filename: str | Path) -> "SnapshotMeta":
        """Load metadata from YAML file. Metadata must be stored under `meta` key."""
        with open(filename) as istream:
            yml = yaml.safe_load(istream)
            if "meta" not in yml:
                raise ValueError("Metadata YAML should be stored under `meta` key")
            meta = yml["meta"]

            # Always derive these fields from the path. If the YAML provides them explicitly,
            # they must match — otherwise rename the file or remove the override.
            namespace, version, short_name, file_extension = _parse_snapshot_path(Path(filename))
            for key, derived in (
                ("namespace", namespace),
                ("version", version),
                ("short_name", short_name),
                ("file_extension", file_extension),
            ):
                if key in meta and str(meta[key]) != str(derived):
                    raise ValueError(
                        f"{filename}: YAML field '{key}'={meta[key]!r} does not match the value "
                        f"derived from the filename ({derived!r}). Rename the file or remove the override."
                    )
            meta["namespace"] = namespace
            meta["version"] = version
            meta["short_name"] = short_name
            meta["file_extension"] = file_extension

            if "origin" in meta:
                meta["origin"] = Origin.from_dict(meta["origin"])

            assert meta.get("origin"), '"origin" must be set'

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

    def to_table_metadata(self):
        assert self.origin, f"Snapshot {self.uri} must have an origin"
        return TableMeta.from_dict(
            {
                "short_name": self.short_name,
                "title": self.origin.title,
                "description": self.origin.description,
                "dataset": DatasetMeta.from_dict(
                    {
                        "channel": "snapshots",
                        "namespace": self.namespace,
                        "short_name": self.short_name,
                        "title": self.origin.title,
                        "description": self.origin.description,
                        "licenses": [self.license] if self.license else [],
                        "is_public": self.is_public,
                        "version": self.version,
                    }
                ),
            }
        )


def read_table_from_snapshot(
    path: str | Path,
    table_metadata: TableMeta,
    snapshot_origin: Origin | None,
    file_extension: str,
    safe_types: bool = True,
    read_function: Callable | None = None,
    *args,
    **kwargs,
) -> Table:
    """Read snapshot as a table."""
    # Define kwargs / args
    args = [
        path,
        *args,
    ]
    kwargs = {
        **kwargs,
        "metadata": table_metadata,
        "origin": snapshot_origin,
    }
    # Read table
    if read_function is not None:
        tb = pr.read_custom(read_function, *args, **kwargs)
    else:
        tb = pr.read(*args, file_extension=file_extension, **kwargs)

    if safe_types:
        tb = cast(Table, to_safe_types(tb))

    return tb


def snapshot_catalog(match: str = r".*") -> Iterator[Snapshot]:
    """Return a catalog of all snapshots. It can take more than 10s to load the entire catalog,
    so it's recommended to use `match` to filter the snapshots.
    :param match: pattern to match uri
    """
    for path in paths.SNAPSHOTS_DIR.glob("**/*.dvc"):
        uri = str(path.relative_to(paths.SNAPSHOTS_DIR)).replace(".dvc", "")
        if re.search(match, uri):
            yield Snapshot(uri)


_SHORT_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _parse_snapshot_path(path: Path) -> tuple[str, str, str, str]:
    """Parse snapshot path into namespace, short_name, file_extension."""
    version = path.parent.name
    namespace = path.parent.parent.name

    short_name, ext = path.stem.split(".", 1)
    assert "." not in ext, f"{path.name} cannot contain `.`"
    assert _SHORT_NAME_RE.match(short_name), (
        f"{path.name}: short_name {short_name!r} must be snake_case (matching {_SHORT_NAME_RE.pattern})"
    )
    return namespace, version, short_name, ext
