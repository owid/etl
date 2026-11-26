from pathlib import Path

from apps.autoupdate.cli import SnapshotUpdate, group_assignees


def _update(*assignees: str) -> SnapshotUpdate:
    return SnapshotUpdate(name="Epoch", snapshot_script=Path("x.py"), dvc_files=[], assignees=list(assignees))


def test_group_assignees_merges_snapshots_in_order_without_duplicates():
    updates = [_update("lucasrodes"), _update(), _update("edomt", "lucasrodes")]
    assert group_assignees(updates) == ["lucasrodes", "edomt"]


def test_group_assignees_empty_when_no_snapshot_lists_any():
    assert group_assignees([_update(), _update()]) == []
