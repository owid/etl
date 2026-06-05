from datetime import datetime, timedelta
from types import SimpleNamespace

from apps.wizard.app_pages.chart_diff.chart_diff import (
    ChartDiff,
    _is_catalog_path_twin,
    _same_chart_across_envs,
    _target_updated_at_for_review,
)


def _chart(chart_id: int, created_at: datetime, catalog_path: str | None = None):
    return SimpleNamespace(
        id=chart_id,
        createdAt=created_at,
        updatedAt=created_at + timedelta(hours=1),
        catalogPath=catalog_path,
    )


def test_catalog_path_identifies_etl_chart_twins():
    source = _chart(100, datetime(2026, 1, 1), "animal_welfare/latest/chart#chart")
    target = _chart(200, datetime(2026, 1, 2), "animal_welfare/latest/chart#chart")

    assert _same_chart_across_envs(source, target)
    assert _is_catalog_path_twin(source, target)
    assert _target_updated_at_for_review(source, target) is None

    diff = ChartDiff(source_chart=source, target_chart=target, approval=None, conflict=None)
    assert diff.chart_id == source.id


def test_regular_charts_still_match_by_id_and_created_at():
    created_at = datetime(2026, 1, 1)
    source = _chart(100, created_at)
    target = _chart(100, created_at)

    assert _same_chart_across_envs(source, target)
    assert not _is_catalog_path_twin(source, target)
    assert _target_updated_at_for_review(source, target) == target.updatedAt
