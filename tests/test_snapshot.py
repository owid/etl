from pathlib import Path

import pytest
from owid.catalog import Origin

from etl.snapshot import SnapshotMeta, _parse_snapshot_path


def test_parse_snapshot_path():
    path = Path("etl/snapshots/aviation_safety_network/2023-04-18/aviation_statistics_by_period.csv.dvc")
    assert _parse_snapshot_path(path) == (
        "aviation_safety_network",
        "2023-04-18",
        "aviation_statistics_by_period",
        "csv",
    )

    # snapshot names shouldn't contain dot
    with pytest.raises(AssertionError):
        path = Path("etl/snapshots/unep/2023-03-17/consumption_controlled_substances.hydrobromofluorocarbons.xlsx.dvc")
        _parse_snapshot_path(path)


def test_snapshot_to_yaml():
    d = SnapshotMeta(
        namespace="aviation_safety_network",
        version="2023-04-18",
        short_name="aviation_statistics_by_period",
        file_extension="csv",
        origin=Origin(producer="Producer", title="Aviation Statistics by Period"),
    ).to_dict()
    assert d == {
        "file_extension": "csv",
        "is_public": True,
        "namespace": "aviation_safety_network",
        "short_name": "aviation_statistics_by_period",
        "version": "2023-04-18",
        "origin": {"title": "Aviation Statistics by Period", "producer": "Producer"},
    }
