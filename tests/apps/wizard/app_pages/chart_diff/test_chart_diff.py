from datetime import datetime, timedelta
from types import SimpleNamespace

# NOTE: chart_diff is imported lazily inside each test, not at module top.
# Importing it applies its module-level @st_cache_data decorators, which call
# is_running_in_streamlit() — a @cache'd function frozen on first call. During
# pytest collection (no Streamlit runtime) that freezes it to False process-wide,
# which routes other apps' cached functions onto the non-Streamlit path and
# breaks unrelated integration tests (e.g. producer_analytics). Keeping the
# import inside the test bodies means collection never triggers it.


def _chart(chart_id: int, created_at: datetime, catalog_path: str | None = None):
    return SimpleNamespace(
        id=chart_id,
        createdAt=created_at,
        updatedAt=created_at + timedelta(hours=1),
        catalogPath=catalog_path,
    )


def test_catalog_path_identifies_etl_chart_twins(monkeypatch):
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        ChartDiff,
        _is_catalog_path_twin,
        _same_chart_across_envs,
        _target_updated_at_for_review,
    )

    # catalogPath matching only kicks in when the target (prod) DB has the
    # columns; the SimpleNamespace charts here aren't bound to a session, so
    # force the detection on to exercise the logic.
    monkeypatch.setattr(
        "apps.wizard.app_pages.chart_diff.chart_diff._target_has_etl_columns",
        lambda _target_chart: True,
    )
    source = _chart(100, datetime(2026, 1, 1), "animal_welfare/latest/chart#chart")
    target = _chart(200, datetime(2026, 1, 2), "animal_welfare/latest/chart#chart")

    assert _same_chart_across_envs(source, target)
    assert _is_catalog_path_twin(source, target)
    assert _target_updated_at_for_review(source, target) is None

    diff = ChartDiff(source_chart=source, target_chart=target, approval=None, conflict=None)
    assert diff.chart_id == source.id


def test_regular_charts_still_match_by_id_and_created_at():
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        _is_catalog_path_twin,
        _same_chart_across_envs,
        _target_updated_at_for_review,
    )

    created_at = datetime(2026, 1, 1)
    source = _chart(100, created_at)
    target = _chart(100, created_at)

    assert _same_chart_across_envs(source, target)
    assert not _is_catalog_path_twin(source, target)
    assert _target_updated_at_for_review(source, target) == target.updatedAt
