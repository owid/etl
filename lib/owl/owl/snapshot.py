"""
Snapshot: version-pinned raw data for Owl steps.

Owl step code lives under:
    owl_steps/<namespace>/<dataset>/vYYYYMMDD/step.py

Snapshot files are stored in the existing ETL data tree:
    data/snapshots/<namespace>/<snapshot-version>/<dataset>__<snapshot-name>.parquet

Metadata is loaded from either a sibling ``meta.yml`` file or from the
legacy ``step.meta.yml`` sidecar. The YAML can have a top-level
``snapshots:`` section keyed by snapshot function name.
"""

from __future__ import annotations

import json
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

from owl.project import find_steps_root, load_project, parse_step_file


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
            import yaml

            with yaml_path.open() as f:
                return yaml.safe_load(f) or {}
    return {}


class Snapshot:
    """Decorator for version-pinned raw data snapshots.

    Usage:
        @Snapshot(version="2024-12-31")
        def raw_data():
            return fetch_data()

        raw_data.load()           # loads pinned version
        raw_data.fetch_and_save() # fetches and writes to data/snapshots/
    """

    is_snapshot = True

    def __init__(self, *, version: str):
        self.version = version
        self._meta: dict | None = None
        self._fn = None
        self.name = None
        self._source_file = None

    def __call__(self, fn):
        """Decorator: bind to a fetch function."""
        self._fn = fn
        self.name = fn.__name__
        self._source_file = fn.__globals__["__file__"]
        return self

    @property
    def meta(self) -> dict:
        """Snapshot metadata from the step metadata file, lazy-loaded."""
        if self._meta is None:
            all_meta = _load_yaml_sidecar(self._source_file)
            snapshots_section = all_meta.get("snapshots", {})
            self._meta = snapshots_section.get(self.name, {})
        return self._meta

    def _version_path(self, version: str) -> pathlib.Path:
        """Path to a specific version's parquet file."""
        project = load_project(pathlib.Path(self._source_file).parent)
        info = parse_step_file(self._source_file)
        filename = f"{info.dataset}__{self.name}.parquet"
        return project.snapshots_root / info.namespace / version / filename

    @property
    def _path(self) -> pathlib.Path:
        """Path to the pinned snapshot parquet file."""
        return self._version_path(self.version)

    def load(self, version: str | None = None):
        """Load DataFrame (and metadata if present) from the snapshot parquet."""
        path = self._version_path(version or self.version)
        if not path.exists():
            step_hint = _step_rel_path(self._source_file)
            raise FileNotFoundError(f"Snapshot not found: {path}\nRun: owl snapshot {step_hint}")
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

        if meta:
            return df, meta
        return df

    def fetch_and_save(self, version: str | None = None):
        """Call the fetch function and write the result to a parquet file."""
        resolved_version = version or self.version
        path = self._version_path(resolved_version)

        assert self._fn is not None
        result = self._fn()

        if isinstance(result, tuple):
            df, meta = result
        else:
            df, meta = result, {}

        if self.meta:
            meta["snapshot_meta"] = self.meta

        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pandas(df)

        if meta:
            arrow_meta = {k: json.dumps(v) if not isinstance(v, str) else v for k, v in meta.items()}
            table = table.replace_schema_metadata(arrow_meta)

        pq.write_table(table, path, compression="snappy")
        from owl.log import snapshot as _log_snapshot

        _log_snapshot(f"wrote {path.name} ({len(df)} rows)")

    def __repr__(self):
        if self._source_file:
            return f"Snapshot({_step_rel_path(self._source_file)}/{self.name}, version={self.version})"
        return f"Snapshot(version={self.version}, unbound)"
