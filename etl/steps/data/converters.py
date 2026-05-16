#
#  converters.py
#

from owid.catalog import DatasetMeta

from etl.snapshot import SnapshotMeta


def convert_snapshot_metadata(snap: SnapshotMeta) -> DatasetMeta:
    """
    Copy metadata for a dataset directly from what we have in Snapshot.
    """
    assert snap.origin, f"Snapshot {snap.uri} must have an origin"
    ds_meta = DatasetMeta(
        short_name=snap.short_name,
        namespace=snap.namespace,
        version=snap.version,
        title=snap.origin.title,
        description=snap.origin.description,
        licenses=[snap.license] if snap.license else [],
    )
    ds_meta.origins = [snap.origin]

    return ds_meta
