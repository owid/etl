from pathlib import Path

from etl.snapshot import _parse_snapshot_path


def test_parse_snapshot_path():
    path = Path("etl/snapshots/aviation_safety_network/2023-04-18/aviation_statistics_by_period.csv.dvc")
    namespace, version, short_name, ext = _parse_snapshot_path(path)
    assert namespace == "aviation_safety_network"
    assert version == "2023-04-18"
    assert short_name == "aviation_statistics_by_period"
    assert ext == "csv"
