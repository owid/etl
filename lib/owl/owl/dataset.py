"""
Dataset & Action: the two executable step types in Owl.

Owl step code lives under:
    owl_steps/<namespace>/<dataset>/vYYYYMMDD/step.py

Datasets are written with the existing OWID catalog machinery to:
    data/garden/<namespace>/YYYY-MM-DD/<dataset>/

Snapshots are written by ``Snapshot`` to:
    data/snapshots/<namespace>/<snapshot-version>/<dataset>__<snapshot-name>.parquet
"""

from __future__ import annotations

import inspect
import pathlib
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd
from owid import catalog
from owid.catalog import Table
from owid.catalog.core.meta import DatasetMeta as CatalogDatasetMeta
from owid.catalog.core.meta import VariableMeta

from owl.project import dataset_output_dir, load_project, parse_step_file
from owl.snapshot import Snapshot, _load_yaml_sidecar, _step_rel_path


@dataclass
class ColumnMeta:
    """Metadata for a single dataset column."""

    title: str = ""
    description: str = ""
    unit: str = ""
    role: str = ""  # "entity" | "time" | "dimension" | "metric" | ""


@dataclass
class DatasetMeta:
    """Small, Owl-native metadata object returned alongside a DataFrame."""

    description: str = ""
    source: str = ""
    title: str = ""
    tags: list[str] = field(default_factory=list)
    columns: dict[str, ColumnMeta] = field(default_factory=dict)
    default_entities: list[str] = field(default_factory=list)


def _validate_meta(meta: DatasetMeta, df: pd.DataFrame | None = None) -> None:
    """Validate metadata constraints. Raises AssertionError on violations."""
    entity_cols = [name for name, col in meta.columns.items() if col.role == "entity"]
    assert len(entity_cols) <= 1, (
        f"At most one column may have role='entity', but found {len(entity_cols)}: {entity_cols}"
    )

    for name, col in meta.columns.items():
        assert col.title, (
            f"Column '{name}' is missing a title. Add title= to ColumnMeta (short label, e.g. 'Price-to-income ratio')."
        )

    if df is not None:
        allowed_time_dtypes = {
            "int64",
            "int32",
            "Int64",
            "Int32",
            "float64",
            "object",
            "string",
            "str",
        }
        time_cols = [name for name, col in meta.columns.items() if col.role == "time"]
        for col_name in time_cols:
            if col_name not in df.columns:
                continue
            dtype = str(df[col_name].dtype)
            assert dtype in allowed_time_dtypes, (
                f"Column '{col_name}' has role='time' but dtype '{dtype}'. "
                f"Time columns must be int (year), or string 'YYYY-MM-DD' (date). "
                f"Use .strftime('%Y-%m-%d') to convert timestamps to date strings. "
                f"Allowed dtypes: {allowed_time_dtypes}"
            )


def _datasetmeta_to_dict(meta: DatasetMeta) -> dict[str, Any]:
    """Convert DatasetMeta to the same nested dict shape as Owl YAML."""
    d: dict[str, Any] = {}
    ds: dict[str, Any] = {}
    if meta.description:
        ds["description"] = meta.description
    if meta.source:
        ds["source"] = meta.source
    if meta.title:
        ds["title"] = meta.title
    if meta.tags:
        ds["tags"] = meta.tags
    if meta.default_entities:
        ds["default_entities"] = meta.default_entities
    if ds:
        d["dataset"] = ds
    if meta.columns:
        d["columns"] = {name: {k: v for k, v in asdict(col).items() if v} for name, col in meta.columns.items()}
    return d


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Override wins on conflicts."""
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _dataset_meta_from_dict(meta: dict[str, Any]) -> CatalogDatasetMeta:
    """Convert Owl dataset metadata into owid.catalog DatasetMeta."""
    dataset = meta.get("dataset", {})
    additional_info = {}
    for key in ["source", "tags", "default_entities"]:
        if key in dataset:
            additional_info[key] = dataset[key]

    return CatalogDatasetMeta(
        title=dataset.get("title"),
        description=dataset.get("description"),
        additional_info=additional_info or None,
    )


def _variable_meta_from_dict(meta: dict[str, Any]) -> VariableMeta:
    """Convert Owl column metadata into owid.catalog VariableMeta."""
    known = {
        "title",
        "description",
        "description_short",
        "description_from_producer",
        "description_key",
        "origins",
        "licenses",
        "unit",
        "short_unit",
        "display",
        "additional_info",
        "processing_level",
        "presentation",
        "description_processing",
        "license",
        "sources",
        "type",
        "sort",
        "dimensions",
        "original_short_name",
        "original_title",
    }
    variable_meta: dict[str, Any] = {}
    additional_info = dict(meta.get("additional_info") or {})

    for key, value in meta.items():
        if key == "role":
            additional_info["role"] = value
        elif key == "description":
            variable_meta["description_short"] = value
        elif key in known:
            variable_meta[key] = value
        else:
            additional_info[key] = value

    if additional_info:
        variable_meta["additional_info"] = additional_info

    return VariableMeta.from_dict(variable_meta)


def _prepare_table(df: pd.DataFrame | Table, name: str, meta: dict[str, Any]) -> Table:
    """Create an owid.catalog Table and attach variable metadata."""
    if isinstance(df, Table):
        tb = df.copy()
        tb.metadata.short_name = tb.metadata.short_name or name
    else:
        tb = Table(df, short_name=name)

    columns_meta = meta.get("columns", {})
    for col_name, col_meta in columns_meta.items():
        if col_name in tb.columns:
            tb[col_name].metadata = _variable_meta_from_dict(col_meta)

    primary_key = [
        col_name
        for col_name, col_meta in columns_meta.items()
        if col_meta.get("role") in {"entity", "time", "dimension"} and col_name in tb.columns
    ]
    if primary_key:
        tb = tb.set_index(primary_key, verify_integrity=True)
        tb.metadata.short_name = name
        tb.metadata.dimensions = [
            {"name": columns_meta.get(key, {}).get("title") or key, "slug": key} for key in primary_key
        ]

    return tb


class Dataset:
    """Decorator: turns a function into a catalog dataset-producing step."""

    is_dataset = True

    def __init__(self, fn=None, *, deps=None, channel: str | None = None):
        self._fn = None
        self._explicit_deps = deps
        self.channel = channel
        self.name = None
        self._source_file = None
        self.path = None
        self._meta: dict | None = None

        if fn is not None:
            self._init_from_fn(fn)

    def __call__(self, *args, **kwargs):
        if self._fn is None:
            fn = args[0]
            self._init_from_fn(fn)
            return self
        return self._fn(*args, **kwargs)

    def _init_from_fn(self, fn):
        self._fn = fn
        self.name = fn.__name__
        self._source_file = fn.__globals__["__file__"]
        self.path = _step_rel_path(self._source_file)

    @property
    def meta(self) -> dict:
        """Dataset metadata from the Owl metadata file, lazy-loaded."""
        if self._meta is None:
            all_meta = _load_yaml_sidecar(self._source_file)
            self._meta = all_meta.get("datasets", {}).get(self.name, {})
        return self._meta

    @property
    def _data_path(self) -> pathlib.Path:
        return dataset_output_dir(self._source_file, self.name, self.channel)

    @property
    def _mtime_path(self) -> pathlib.Path:
        return self._data_path / "index.json"

    def _dependencies(self) -> list:
        """Extract Snapshot/Dataset objects from function parameters."""
        if self._explicit_deps is not None:
            return list(self._explicit_deps)
        assert self._fn is not None
        sig = inspect.signature(self._fn)
        module_globals = self._fn.__globals__
        deps = []
        for param_name in sig.parameters:
            obj = module_globals.get(param_name)
            if isinstance(obj, (Snapshot, Dataset)):
                deps.append(obj)
        return deps

    def _resolve_kwargs(self) -> dict:
        """Map dependencies to function parameter names."""
        deps = self._dependencies()
        if self._explicit_deps is not None:
            assert self._fn is not None
            params = list(inspect.signature(self._fn).parameters.keys())
            return dict(zip(params, deps))
        assert self._fn is not None
        kwargs = {}
        for dep in deps:
            sig = inspect.signature(self._fn)
            for param_name in sig.parameters:
                obj = self._fn.__globals__.get(param_name)
                if obj is dep:
                    kwargs[param_name] = dep
                    break
        return kwargs

    def is_stale(self) -> bool:
        """Check if this dataset needs to be rebuilt."""
        mtime_path = self._mtime_path
        if not mtime_path.exists():
            return True

        my_mtime = mtime_path.stat().st_mtime

        source = pathlib.Path(self._source_file)
        if source.stat().st_mtime > my_mtime:
            return True

        for meta_path in [source.parent / "meta.yml", source.with_suffix(".meta.yml")]:
            if meta_path.exists() and meta_path.stat().st_mtime > my_mtime:
                return True

        for dep in self._dependencies():
            if isinstance(dep, Snapshot):
                if not dep._path.exists():
                    return True
                if dep._path.stat().st_mtime > my_mtime:
                    return True
            elif isinstance(dep, Dataset):
                if dep.is_stale():
                    return True
                if dep._mtime_path.exists() and dep._mtime_path.stat().st_mtime > my_mtime:
                    return True

        return False

    def load(self) -> pd.DataFrame:
        """Load the dataset's primary table as a pandas DataFrame."""
        path = self._data_path
        if not (path / "index.json").exists():
            raise FileNotFoundError(f"Dataset not found: {path}\nRun: owl run {self.path}")
        from owl.log import dataset as _log_dataset

        _log_dataset(f"loading {path.name}")
        tb = catalog.Dataset(path).read(self.name, reset_index=True, safe_types=False)
        return pd.DataFrame(tb)

    def run(self, force=False):
        """Execute the function and save an ETL-compatible catalog dataset."""
        for dep in self._dependencies():
            if isinstance(dep, Dataset):
                dep.run(force=force)

        if not force and not self.is_stale():
            from owl.log import skip

            skip(self.name)
            return

        assert self._fn is not None
        result = self._fn(**self._resolve_kwargs())

        if isinstance(result, tuple):
            df, inline_meta = result
        else:
            df, inline_meta = result, None

        if inline_meta is not None and isinstance(inline_meta, DatasetMeta):
            _validate_meta(inline_meta, pd.DataFrame(df))
            inline_dict = _datasetmeta_to_dict(inline_meta)
        elif inline_meta is not None:
            inline_dict = dict(inline_meta)
        else:
            inline_dict = {}

        merged = _deep_merge(dict(self.meta), inline_dict)
        table = _prepare_table(df, self.name, merged)
        dataset_meta = _dataset_meta_from_dict(merged)

        from owl.log import dataset as _log_dataset

        out_path = self._data_path
        info = parse_step_file(self._source_file)
        project = load_project(pathlib.Path(self._source_file).parent)
        dataset_meta.channel = self.channel or project.default_channel
        dataset_meta.namespace = info.namespace
        dataset_meta.version = info.version
        dataset_meta.short_name = self.name

        ds = catalog.Dataset.create_empty(out_path, metadata=dataset_meta)
        ds.add(table)
        ds.save()

        _log_dataset(f"wrote {out_path} ({len(df)} rows)")
        return pd.DataFrame(df)

    def run_with_prefect(self, force=False):
        """Run with Prefect tracking — each step becomes a task in a flow."""
        from importlib import import_module

        prefect = import_module("prefect")
        flow = prefect.flow
        task = prefect.task

        dataset_self = self

        @flow(name=dataset_self.name, log_prints=True)
        def dataset_flow():
            @task(name=f"run:{dataset_self.name}")
            def execute():
                return dataset_self.run(force=force)

            return execute()

        return dataset_flow()

    def __repr__(self):
        return f"Dataset({self.path}/{self.name})"


class Action:
    """Decorator: a side-effect step that doesn't produce a dataset."""

    is_action = True

    def __init__(self, fn=None, *, deps=None):
        self._fn = None
        self._explicit_deps = deps
        self.name = None
        self._source_file = None
        self.path = None

        if fn is not None:
            self._init_from_fn(fn)

    def __call__(self, *args, **kwargs):
        if self._fn is None:
            fn = args[0]
            self._init_from_fn(fn)
            return self
        return self._fn(*args, **kwargs)

    def _init_from_fn(self, fn):
        self._fn = fn
        self.name = fn.__name__
        self._source_file = fn.__globals__["__file__"]
        self.path = _step_rel_path(self._source_file)

    @property
    def _stamp_path(self) -> pathlib.Path:
        """Path to the stamp file that records when this action last ran."""
        info = parse_step_file(self._source_file)
        project = load_project(pathlib.Path(self._source_file).parent)
        return (
            project.root
            / ".cache"
            / "owl"
            / "stamps"
            / info.namespace
            / info.dataset
            / info.version
            / f"{self.name}.stamp"
        )

    def _dependencies(self) -> list:
        """Extract Snapshot/Dataset/Action objects from function parameters."""
        if self._explicit_deps is not None:
            return list(self._explicit_deps)
        assert self._fn is not None
        sig = inspect.signature(self._fn)
        module_globals = self._fn.__globals__
        deps = []
        for param_name in sig.parameters:
            obj = module_globals.get(param_name)
            if isinstance(obj, (Snapshot, Dataset, Action)):
                deps.append(obj)
        return deps

    def _resolve_kwargs(self) -> dict:
        """Map dependencies to function parameter names."""
        deps = self._dependencies()
        if self._explicit_deps is not None:
            assert self._fn is not None
            params = list(inspect.signature(self._fn).parameters.keys())
            return dict(zip(params, deps))
        assert self._fn is not None
        kwargs = {}
        for dep in deps:
            sig = inspect.signature(self._fn)
            for param_name in sig.parameters:
                obj = self._fn.__globals__.get(param_name)
                if obj is dep:
                    kwargs[param_name] = dep
                    break
        return kwargs

    def is_stale(self) -> bool:
        """Check if this action needs to re-run."""
        stamp = self._stamp_path
        if not stamp.exists():
            return True

        my_mtime = stamp.stat().st_mtime

        source = pathlib.Path(self._source_file)
        if source.stat().st_mtime > my_mtime:
            return True

        for meta_path in [source.parent / "meta.yml", source.with_suffix(".meta.yml")]:
            if meta_path.exists() and meta_path.stat().st_mtime > my_mtime:
                return True

        for dep in self._dependencies():
            if isinstance(dep, Snapshot):
                if not dep._path.exists():
                    return True
                if dep._path.stat().st_mtime > my_mtime:
                    return True
            elif isinstance(dep, Dataset):
                if dep.is_stale():
                    return True
                if dep._mtime_path.exists() and dep._mtime_path.stat().st_mtime > my_mtime:
                    return True
            elif isinstance(dep, Action):
                if dep.is_stale():
                    return True

        return False

    def run(self, force=False):
        """Run the action if stale (or forced)."""
        if not force and not self.is_stale():
            from owl.log import skip

            skip(self.name)
            return

        assert self._fn is not None
        self._fn(**self._resolve_kwargs())

        self._stamp_path.parent.mkdir(parents=True, exist_ok=True)
        self._stamp_path.write_text(f"ran {self.name}\n")
        from owl.log import action as _log_action

        _log_action(f"{self.name} done")

    def __repr__(self):
        return f"Action({self.path}/{self.name})"
