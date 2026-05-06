"""
Bridge between the lightweight ETL framework and owid-catalog.

Optional — only import this if you use owid-catalog's Table/Origin.
The core framework (snapshot.py, dataset.py) doesn't depend on this.
"""

from __future__ import annotations

import pandas as pd
from owid.catalog import Origin, Table

from owl.snapshot import Snapshot


def load_snapshot(snapshot: Snapshot, short_name: str | None = None) -> Table:
    """Load a Snapshot as an owid-catalog Table with origins attached.

    Wraps snapshot.load() into a Table and, if the snapshot's .meta.yml
    has an "origin" key, attaches it to every column's VariableMeta.

    Args:
        snapshot: A @Snapshot object to load.
        short_name: Table short_name. Defaults to the snapshot's function name.

    Returns:
        Table with data and (if available) origins on all columns.
    """
    result = snapshot.load()
    if isinstance(result, tuple):
        df, _meta = result
    else:
        df = result

    tb = Table(df, short_name=short_name or snapshot.name)

    # Attach origin from .meta.yml if present
    origin_dict = snapshot.meta.get("origin")
    if origin_dict:
        origin = Origin.from_dict(origin_dict)
        for col in tb.columns:
            tb[col].metadata.origins = [origin]

    return tb


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
