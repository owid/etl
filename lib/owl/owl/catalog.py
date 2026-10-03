"""
Bridge between the lightweight ETL framework and owid-catalog.

Optional — only import this if you use owid-catalog's Table/Origin.
The core framework (snapshot.py, dataset.py) doesn't depend on this.
"""

from __future__ import annotations

import pandas as pd
from owid.catalog import DatasetMeta, Origin, Table, TableMeta
from owid.catalog import processing as pr

from owl.snapshot import Snapshot


def _origin_from_snapshot(snapshot: Snapshot) -> Origin | None:
    origin_dict = snapshot.meta.get("origin")
    return Origin.from_dict(origin_dict) if origin_dict else None


def _read_snapshot(snapshot: Snapshot, *, short_name: str | None = None, **kwargs) -> Table:
    path = snapshot.path()
    metadata = TableMeta(short_name=short_name or snapshot.name, dataset=DatasetMeta())
    return pr.read(path, metadata=metadata, origin=_origin_from_snapshot(snapshot), **kwargs)


def load_snapshot(snapshot: Snapshot, short_name: str | None = None, **kwargs) -> Table:
    """Load a Snapshot as an owid-catalog Table with origins attached.

    Uses owid-catalog processing readers and, if the snapshot's ``meta.yml``
    has an ``origin`` key, passes it to the reader so origins are attached to every column.

    Args:
        snapshot: A @Snapshot object to load.
        short_name: Table short_name. Defaults to the snapshot's function name.
        **kwargs: Passed to the inferred pandas reader.

    Returns:
        Table with data and (if available) origins on all columns.
    """
    return _read_snapshot(snapshot, short_name=short_name, **kwargs)


def export(tb: Table) -> tuple[pd.DataFrame, dict]:
    """Convert an owid-catalog Table into (DataFrame, metadata dict).

    Extracts the rich per-column VariableMeta (origins, units, descriptions,
    etc.) from the Table and returns it as a plain dict alongside the data.
    This is the return value a @Dataset step expects.

    Usage in a step:
        @Dataset
        def my_dataset(...):
            ...
            return export(tb)

    The metadata dict has a "columns" key with per-column VariableMeta
    serialized as dicts. Any existing YAML metadata from .meta.yml is
    merged on top by Dataset.run() — YAML fields act as base, the
    VariableMeta extracted here fills in everything else (origins, etc.).
    """
    columns_meta = {}
    for col in tb.all_columns:
        col_meta = tb.get_column_or_index(col).metadata.to_dict()
        # Drop empty values to keep it clean
        col_meta = {k: v for k, v in col_meta.items() if v}
        if col_meta:
            columns_meta[col] = col_meta

    meta: dict = {}
    if columns_meta:
        meta["columns"] = columns_meta

    return pd.DataFrame(tb), meta
