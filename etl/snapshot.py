#
#  snapshot.py
#
#  Re-exports from owid.etl.snapshot for backwards compatibility.
#

# Import directly from the module to avoid circular imports through __init__.py
from owid.etl.snapshot import (
    Snapshot,
    SnapshotMeta,
    read_table_from_snapshot,
    snapshot_catalog,
)

__all__ = [
    "Snapshot",
    "SnapshotMeta",
    "read_table_from_snapshot",
    "snapshot_catalog",
]
