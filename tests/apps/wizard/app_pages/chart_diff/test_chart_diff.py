from datetime import datetime, timedelta
from types import SimpleNamespace

# NOTE: chart_diff is imported lazily inside each test, not at module top.
# Importing it applies its module-level @st_cache_data decorators, which call
# is_running_in_streamlit() — a @cache'd function frozen on first call. During
# pytest collection (no Streamlit runtime) that freezes it to False process-wide,
# which routes other apps' cached functions onto the non-Streamlit path and
# breaks unrelated integration tests (e.g. producer_analytics). Keeping the
# import inside the test bodies means collection never triggers it.


def _chart(chart_id: int, created_at: datetime, catalog_path: str | None = None, config_id: str | None = None):
    return SimpleNamespace(
        id=chart_id,
        createdAt=created_at,
        updatedAt=created_at + timedelta(hours=1),
        etlConfigCatalogPath=catalog_path,
        configId=config_id or f"config-uuid-{chart_id}",
    )


def test_config_uuid_identifies_chart_twins():
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        ChartDiff,
        _is_cross_env_twin,
        _same_chart_across_envs,
        _target_updated_at_for_review,
    )

    # Same config UUID, different numeric ids: a chart synced from staging to
    # production. No ETL columns needed — configId exists everywhere.
    config_id = "0198c0e8-0000-7000-8000-000000000000"
    source = _chart(100, datetime(2026, 1, 1), config_id=config_id)
    target = _chart(200, datetime(2026, 1, 2), config_id=config_id)

    assert _same_chart_across_envs(source, target)
    assert _is_cross_env_twin(source, target)
    assert _target_updated_at_for_review(source, target) is None

    diff = ChartDiff(source_chart=source, target_chart=target, approval=None, conflict=None)
    assert diff.chart_id == source.id


def test_config_uuid_matching_ignores_case():
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        _is_cross_env_twin,
        _same_chart_across_envs,
        same_config_uuid,
    )

    # UUIDs are case-insensitive, and the grapher admin accepts a caller-supplied
    # UUID in any case — so the same chart may be stored upper-case in one
    # environment and lower-case in another.
    source = _chart(100, datetime(2026, 1, 1), config_id="0198C0E8-0000-7000-8000-000000000000")
    target = _chart(200, datetime(2026, 1, 2), config_id="0198c0e8-0000-7000-8000-000000000000")

    assert same_config_uuid(source.configId, target.configId)
    assert not same_config_uuid(source.configId, None)
    assert not same_config_uuid(None, None)
    assert _same_chart_across_envs(source, target)
    assert _is_cross_env_twin(source, target)


def test_catalog_path_identifies_etl_chart_twins(monkeypatch):
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        ChartDiff,
        _is_cross_env_twin,
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
    assert _is_cross_env_twin(source, target)
    assert _target_updated_at_for_review(source, target) is None

    diff = ChartDiff(source_chart=source, target_chart=target, approval=None, conflict=None)
    assert diff.chart_id == source.id


def test_regular_charts_still_match_by_id_and_created_at():
    from apps.wizard.app_pages.chart_diff.chart_diff import (
        _is_cross_env_twin,
        _same_chart_across_envs,
        _target_updated_at_for_review,
    )

    created_at = datetime(2026, 1, 1)
    # Different config UUIDs (e.g. rows written before configIds were carried
    # across environments would still differ) — the id+createdAt fallback
    # must keep matching these.
    source = _chart(100, created_at, config_id="config-uuid-source")
    target = _chart(100, created_at, config_id="config-uuid-target")

    assert _same_chart_across_envs(source, target)
    assert not _is_cross_env_twin(source, target)
    assert _target_updated_at_for_review(source, target) == target.updatedAt


# --- What a sync would drop from the target's authored layer ------------------
#
# A chart created on a staging branch has no counterpart in production when it is reviewed, so
# its approval records no production version and never goes stale. Production's own ETL creates
# the chart after the merge; if someone edits it there before chart-sync runs, the approval still
# matches. `patch_keys_lost_by_sync` is what tells chart-sync to refuse instead of overwriting.

# What grapher puts in a chart's authored layer when the ETL creates it: the slug, plus
# bookkeeping it writes itself.
_BOOTSTRAP_PATCH = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.011.json",
    "id": 9231,
    "version": 1,
    "slug": "whales-caught",
}


def test_freshly_created_target_chart_loses_nothing():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_keys_lost_by_sync

    # Production's chart as its ETL just created it; staging carries an admin edit to sync.
    assert patch_keys_lost_by_sync(_BOOTSTRAP_PATCH, {**_BOOTSTRAP_PATCH, "note": "Added on staging."}) == []


def test_edit_made_in_the_target_admin_is_reported():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_keys_lost_by_sync

    target = {**_BOOTSTRAP_PATCH, "subtitle": "Hotfixed in production.", "note": "And a note."}
    source = {**_BOOTSTRAP_PATCH, "note": "And a note."}
    assert patch_keys_lost_by_sync(target, source) == ["subtitle"]


def test_bookkeeping_keys_are_not_reported():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_keys_lost_by_sync

    # `id`, `version` and `$schema` are written by grapher, not by a person, and the two
    # environments hand out their own — so the target can carry them while the source doesn't.
    # Reporting those would block every sync.
    assert patch_keys_lost_by_sync(_BOOTSTRAP_PATCH, {"slug": "whales-caught"}) == []


def test_keys_present_on_both_sides_are_not_reported():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_keys_lost_by_sync

    # The source wins on a shared key by design — that difference is the change the reviewer
    # approved in chart-diff (here, a slug renamed on staging), not something being lost.
    source = {**_BOOTSTRAP_PATCH, "slug": "whales-caught-renamed-on-staging"}
    assert patch_keys_lost_by_sync(_BOOTSTRAP_PATCH, source) == []


def test_missing_patches_are_handled():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_keys_lost_by_sync

    assert patch_keys_lost_by_sync(None, None) == []
    assert patch_keys_lost_by_sync({}, _BOOTSTRAP_PATCH) == []
    assert patch_keys_lost_by_sync({"note": "Only in the target."}, None) == ["note"]
