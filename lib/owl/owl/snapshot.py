"""R2-backed, locally cached raw snapshots for Owl steps."""

from __future__ import annotations

import hashlib
import inspect
import json
import pathlib
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from owl.project import find_steps_root, load_project, parse_step_file


@dataclass(frozen=True)
class CapturedFile:
    """A single raw file captured by a snapshot recipe."""

    path: pathlib.Path
    suffix: str


class SnapshotCapture:
    """Helper passed to ``@Snapshot`` functions when capturing a raw file."""

    def __init__(self, work_dir: pathlib.Path):
        self.work_dir = work_dir
        self.file: CapturedFile | None = None

    def _set_file(self, path: pathlib.Path, suffix: str | None = None) -> pathlib.Path:
        if self.file is not None:
            raise ValueError("Each Owl snapshot must capture exactly one file. Bundle multiple files into a zip.")
        resolved_suffix = suffix if suffix is not None else path.suffix
        self.file = CapturedFile(path=path, suffix=resolved_suffix)
        return path

    def add(self, path: str | pathlib.Path, *, suffix: str | None = None) -> pathlib.Path:
        """Register an already-downloaded file as this snapshot."""
        return self._set_file(pathlib.Path(path), suffix=suffix)

    def download(self, url: str, *, suffix: str | None = None) -> pathlib.Path:
        """Download a URL into the capture workspace and register it."""
        inferred_suffix = suffix or pathlib.Path(urllib.parse.urlparse(url).path).suffix
        path = self.work_dir / f"snapshot{inferred_suffix}"
        with urllib.request.urlopen(url) as response, path.open("wb") as f:
            shutil.copyfileobj(response, f)
        return self._set_file(path, suffix=inferred_suffix)

    def write_bytes(self, data: bytes, *, suffix: str = "") -> pathlib.Path:
        """Write bytes into the capture workspace and register the file."""
        path = self.work_dir / f"snapshot{suffix}"
        path.write_bytes(data)
        return self._set_file(path, suffix=suffix)

    def write_text(self, data: str, *, suffix: str = ".txt", encoding: str = "utf-8") -> pathlib.Path:
        """Write text into the capture workspace and register the file."""
        path = self.work_dir / f"snapshot{suffix}"
        path.write_text(data, encoding=encoding)
        return self._set_file(path, suffix=suffix)

    def write_dataframe(self, df: pd.DataFrame, *, suffix: str = ".parquet") -> pathlib.Path:
        """Write a generated DataFrame as a raw snapshot file."""
        path = self.work_dir / f"snapshot{suffix}"
        if suffix == ".parquet":
            pq.write_table(pa.Table.from_pandas(df), path, compression="snappy")
        elif suffix == ".csv":
            df.to_csv(path, index=False)
        else:
            raise ValueError("DataFrame snapshots currently support suffix='.parquet' or suffix='.csv'.")
        return self._set_file(path, suffix=suffix)


def _find_steps_root(source_file: str) -> pathlib.Path:
    """Find the configured Owl steps root for a source file."""
    return find_steps_root(source_file)


def _step_rel_path(source_file: str) -> str:
    """Return a display path like 'biodiversity/cherry_blossom/v20260416'."""
    info = parse_step_file(source_file)
    return f"{info.namespace}/{info.dataset}/{info.version_slug}"


def _load_yaml_sidecar(source_file: str) -> dict:
    """Load metadata next to a step file. Returns {} if missing."""
    source = pathlib.Path(source_file)
    candidates = [source.parent / "meta.yml", source.with_suffix(".meta.yml")]
    for yaml_path in candidates:
        if yaml_path.exists():
            with yaml_path.open() as f:
                return yaml.safe_load(f) or {}
    return {}


def _lock_path(source_file: str) -> pathlib.Path:
    return pathlib.Path(source_file).parent / "snapshot.lock.yml"


def _read_snapshot_lock(source_file: str) -> dict[str, Any]:
    path = _lock_path(source_file)
    if not path.exists():
        return {"snapshots": {}}
    with path.open() as f:
        return yaml.safe_load(f) or {"snapshots": {}}


def _write_snapshot_lock(source_file: str, data: dict[str, Any]) -> None:
    _lock_path(source_file).write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _hash_file(path: pathlib.Path) -> tuple[str, int]:
    md5 = hashlib.md5(usedforsecurity=False)
    size = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            size += len(chunk)
            md5.update(chunk)
    return md5.hexdigest(), size


def _r2_key(md5: str) -> str:
    return f"{md5[:2]}/{md5[2:]}"


def _rclone_uri(remote: str, key: str) -> str:
    return f"{remote.rstrip('/')}/{key}"


def _normalise_result(result: Any, capture: SnapshotCapture) -> CapturedFile:
    if result is not None:
        if isinstance(result, (str, pathlib.Path)):
            capture.add(result)
        elif isinstance(result, pd.DataFrame):
            capture.write_dataframe(result)
        else:
            raise TypeError(
                "Snapshot functions must use the capture helper, return a file path, or return a pandas DataFrame."
            )
    if capture.file is None:
        raise RuntimeError("Snapshot did not capture a file")
    return capture.file


class Snapshot:
    """Decorator for a pinned, single-file raw snapshot.

    Usage:
        @Snapshot
        def raw_data(snap):
            snap.download("https://example.com/raw.csv")

        raw_data.read_csv()       # load from local cache, restoring from R2 if needed
        raw_data.fetch_and_save() # fetch upstream, cache, upload to R2, write lock
    """

    is_snapshot = True

    def __init__(self, fn=None):
        self._meta: dict | None = None
        self._fn = None
        self.name = None
        self._source_file = None
        if fn is not None:
            self._init_from_fn(fn)

    def __call__(self, fn):
        self._init_from_fn(fn)
        return self

    def _init_from_fn(self, fn):
        self._fn = fn
        self.name = fn.__name__
        self._source_file = fn.__globals__["__file__"]

    @property
    def meta(self) -> dict:
        """Snapshot metadata from the step metadata file, lazy-loaded."""
        if self._meta is None:
            all_meta = _load_yaml_sidecar(self._source_file)
            self._meta = all_meta.get("snapshots", {}).get(self.name, {})
        return self._meta

    @property
    def lock_path(self) -> pathlib.Path:
        return _lock_path(self._source_file)

    def _legacy_path(self) -> pathlib.Path:
        project = load_project(pathlib.Path(self._source_file).parent)
        info = parse_step_file(self._source_file)
        version = self.meta.get("origin", {}).get("date_accessed") or self.meta.get("origin", {}).get("date_published")
        if not version:
            version = "unversioned"
        return project.snapshots_root / info.namespace / str(version) / f"{info.dataset}__{self.name}.parquet"

    def _lock_entry(self) -> dict[str, Any] | None:
        return _read_snapshot_lock(self._source_file).get("snapshots", {}).get(self.name)

    def _cache_path_from_entry(self, entry: dict[str, Any]) -> pathlib.Path:
        project = load_project(pathlib.Path(self._source_file).parent)
        suffix = entry.get("suffix") or ""
        return project.snapshots_root / "by-md5" / f"{entry['md5']}{suffix}"

    def identity_mtime(self) -> float:
        """Timestamp for snapshot identity, not local cache freshness."""
        if self.lock_path.exists() and self._lock_entry():
            return self.lock_path.stat().st_mtime
        legacy_path = self._legacy_path()
        if legacy_path.exists():
            return legacy_path.stat().st_mtime
        return 0

    def ensure_cached(self) -> pathlib.Path:
        """Ensure the locked snapshot file exists locally, downloading from R2 if needed."""
        entry = self._lock_entry()
        if not entry:
            legacy_path = self._legacy_path()
            if legacy_path.exists():
                return legacy_path
            raise FileNotFoundError(
                f"Snapshot not found in lock file: {self.name}\nRun: owl snapshot {_step_rel_path(self._source_file)}"
            )

        local_path = self._cache_path_from_entry(entry)
        if local_path.exists():
            md5, size = _hash_file(local_path)
            if md5 == entry.get("md5") and size == entry.get("size"):
                return local_path
            local_path.unlink()

        project = load_project(pathlib.Path(self._source_file).parent)
        remote = project.snapshots_r2_remote
        key = _r2_key(entry["md5"])
        if not remote:
            raise FileNotFoundError(
                f"Snapshot file is not cached locally and no R2 location is configured: {local_path}"
            )

        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(["rclone", "copyto", _rclone_uri(remote, key), str(local_path)], check=True)
        except FileNotFoundError as err:
            raise RuntimeError("rclone is required to download Owl snapshots from R2") from err
        except subprocess.CalledProcessError as err:
            raise RuntimeError(f"Failed to download snapshot from R2: {_rclone_uri(remote, key)}") from err

        md5, size = _hash_file(local_path)
        if md5 != entry.get("md5") or size != entry.get("size"):
            local_path.unlink(missing_ok=True)
            raise RuntimeError(f"Downloaded snapshot failed hash check: {local_path}")
        return local_path

    def path(self) -> pathlib.Path:
        """Return the cached raw snapshot file path, downloading from R2 if needed."""
        return self.ensure_cached()

    def read_csv(self, *args, **kwargs):
        return pd.read_csv(self.path(), *args, **kwargs)

    def read_excel(self, *args, **kwargs):
        return pd.read_excel(self.path(), *args, **kwargs)

    def load(self):
        """Load a legacy parquet snapshot."""
        path = self.path()
        if path.suffix != ".parquet":
            raise ValueError(
                f"Snapshot is stored as raw file {path.name!r}; use .path(), .read_csv(), or .read_excel()."
            )
        from owl.log import snapshot as _log_snapshot

        _log_snapshot(f"loading {path.name}")
        table = pq.read_table(path)
        df = table.to_pandas()
        raw_meta = table.schema.metadata or {}
        meta = {}
        for k, v in raw_meta.items():
            if k.startswith(b"pandas"):
                continue
            key = k.decode()
            val = v.decode()
            try:
                val = json.loads(val)
            except (json.JSONDecodeError, ValueError):
                pass
            meta[key] = val
        meta.pop("snapshot_meta", None)
        return (df, meta) if meta else df

    def fetch_and_save(self):
        """Run snapshot recipe, cache raw file, upload to R2, and update lock."""
        assert self._fn is not None
        with tempfile.TemporaryDirectory() as tmp_dir:
            capture = SnapshotCapture(pathlib.Path(tmp_dir))
            result = self._fn(capture) if inspect.signature(self._fn).parameters else self._fn()
            captured = _normalise_result(result, capture)

            md5, size = _hash_file(captured.path)
            suffix = captured.suffix
            cache_path = (
                load_project(pathlib.Path(self._source_file).parent).snapshots_root / "by-md5" / f"{md5}{suffix}"
            )
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(captured.path, cache_path)

            project = load_project(pathlib.Path(self._source_file).parent)
            if project.snapshots_r2_remote:
                key = _r2_key(md5)
                try:
                    subprocess.run(
                        ["rclone", "copyto", str(cache_path), _rclone_uri(project.snapshots_r2_remote, key)], check=True
                    )
                except FileNotFoundError as err:
                    raise RuntimeError("rclone is required to upload Owl snapshots to R2") from err
                except subprocess.CalledProcessError as err:
                    raise RuntimeError(
                        f"Failed to upload snapshot to R2: {_rclone_uri(project.snapshots_r2_remote, key)}"
                    ) from err

        lock = _read_snapshot_lock(self._source_file)
        snapshots = lock.setdefault("snapshots", {})
        previous = snapshots.get(self.name) or {}
        unchanged = previous.get("md5") == md5 and previous.get("size") == size and previous.get("suffix") == suffix

        from owl.log import snapshot as _log_snapshot

        if unchanged:
            _log_snapshot(f"unchanged {self.name}: {md5}{suffix} ({size} bytes)")
            return

        snapshots[self.name] = {
            "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "md5": md5,
            "size": size,
            "suffix": suffix,
        }
        _write_snapshot_lock(self._source_file, lock)

        _log_snapshot(f"wrote {self.name}: {md5}{suffix} ({size} bytes)")

    def __repr__(self):
        if self._source_file:
            return f"Snapshot({_step_rel_path(self._source_file)}/{self.name})"
        return "Snapshot(unbound)"
