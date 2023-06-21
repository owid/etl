from pathlib import Path

import pytest

from etl.snapshot import _parse_snapshot_path


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
