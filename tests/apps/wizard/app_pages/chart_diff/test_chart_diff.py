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
        catalogPath=catalog_path,
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
    # Approvals bind to the production state that was reviewed — twins included —
    # so a later production edit invalidates a stale approval.
    assert _target_updated_at_for_review(source, target) == target.updatedAt

    diff = ChartDiff(source_chart=source, target_chart=target, approval=None, conflict=None)
    assert diff.chart_id == source.id


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
    assert _target_updated_at_for_review(source, target) == target.updatedAt

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


# --- Layer-aware behavior for ETL-authored charts -----------------------------
#
# Scenario names refer to the situations that motivated the change:
# an ETL-authored chart exists in production, an admin hotfixes it there, and
# other branches' staging servers rebuild the same chart from the YAML.

_SCHEMA_10 = "https://files.ourworldindata.org/schemas/grapher-schema.010.json"
_SCHEMA_11 = "https://files.ourworldindata.org/schemas/grapher-schema.011.json"


def _etl_layer(schema=_SCHEMA_10, variable_id=111):
    return {
        "$schema": schema,
        "title": "Number of whales caught per year",
        "subtitle": "Annual number of whales killed worldwide.",
        "dimensions": [{"property": "y", "variableId": variable_id}],
    }


def _bootstrap_patch(published=False):
    # What a chart's admin patch looks like straight after the ETL created it.
    return {"$schema": _SCHEMA_10, "id": 9220, "version": 3, "slug": "whales-caught", "isPublished": published}


def test_patch_is_pristine():
    from apps.wizard.app_pages.chart_diff.chart_diff import patch_is_pristine

    assert patch_is_pristine(None)
    assert patch_is_pristine({})
    assert patch_is_pristine(_bootstrap_patch())
    # Publishing is a deliberate admin action.
    assert not patch_is_pristine(_bootstrap_patch(published=True))
    # So is any config override.
    assert not patch_is_pristine({**_bootstrap_patch(), "subtitle": "Hotfixed subtitle."})


def test_unrelated_branch_does_not_see_production_hotfix():
    """An admin hotfix in production must not surface as a change on an unrelated branch."""
    from apps.wizard.app_pages.chart_diff.chart_diff import etl_chart_has_branch_changes

    assert not etl_chart_has_branch_changes(
        source_etl=_etl_layer(schema=_SCHEMA_11),  # staging migrated further — still no branch change
        target_etl=_etl_layer(schema=_SCHEMA_10),
        source_patch=_bootstrap_patch(),  # untouched on staging
        target_patch={**_bootstrap_patch(published=True), "subtitle": "Hotfixed subtitle."},
    )


def test_branch_changing_the_code_layer_shows_the_chart():
    """A data update (or YAML edit) on the branch changes the ETL layer — chart is listed."""
    from apps.wizard.app_pages.chart_diff.chart_diff import etl_chart_has_branch_changes

    assert etl_chart_has_branch_changes(
        source_etl=_etl_layer(variable_id=222),  # re-versioned indicator
        target_etl=_etl_layer(variable_id=111),
        source_patch=_bootstrap_patch(),
        target_patch=_bootstrap_patch(published=True),
    )


def test_staging_admin_edit_shows_the_chart():
    """A deliberate edit in the staging admin is the one thing sync must carry."""
    from apps.wizard.app_pages.chart_diff.chart_diff import etl_chart_has_branch_changes

    assert etl_chart_has_branch_changes(
        source_etl=_etl_layer(),
        target_etl=_etl_layer(),
        source_patch={**_bootstrap_patch(), "yAxis": {"min": 0}},
        target_patch=_bootstrap_patch(published=True),
    )


def test_adopted_chart_with_identical_patches_is_hidden():
    """An adopted chart whose (non-pristine) patch matches production has no branch changes."""
    from apps.wizard.app_pages.chart_diff.chart_diff import etl_chart_has_branch_changes

    patch = {**_bootstrap_patch(published=True), "note": "Long-standing admin note."}
    assert not etl_chart_has_branch_changes(
        source_etl=_etl_layer(),
        target_etl=_etl_layer(),
        source_patch=patch,
        target_patch=patch,
    )


def test_twin_conflict_depends_on_staging_patch():
    """Production edits conflict with a twin only once the staging patch carries edits."""
    from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiff

    config_id = "0198c0e8-0000-7000-8000-000000000000"
    staging_created_at = datetime(2026, 1, 1)
    source = _chart(100, datetime(2026, 1, 1), config_id=config_id)
    target = _chart(200, datetime(2026, 1, 2), config_id=config_id)  # edited after staging creation

    # Pristine staging patch: sync will not write the patch, production edits are safe.
    diff = ChartDiff(
        source_chart=source,
        target_chart=target,
        approval=None,
        conflict=None,
        staging_created_at=staging_created_at,
        source_patch_config=_bootstrap_patch(),
    )
    assert not diff.in_conflict

    # Deliberate staging edit: a production edit is a real conflict again.
    diff = ChartDiff(
        source_chart=source,
        target_chart=target,
        approval=None,
        conflict=None,
        staging_created_at=staging_created_at,
        source_patch_config={**_bootstrap_patch(), "subtitle": "Staging override."},
    )
    assert diff.in_conflict
