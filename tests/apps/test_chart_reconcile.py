"""Tests for `etl chart-reconcile` (apps.chart_reconcile).

The database reads aren't covered here (they need a grapher DB). What's tested is the part that
decides what to write: which charts are selected, how admin overrides land in the config YAML,
and how a step is renamed for slug drift.
"""

from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from apps.chart_reconcile.cli import _reconcile as _reconcile_step
from apps.chart_reconcile.cli import _select, _step_for_config_id, cli
from apps.chart_reconcile.reconcile import (
    ChartLayers,
    Result,
    depends_on_dataset,
    dump_config,
    field_overrides,
    find_chart_steps,
    load_config,
    reconcile_fields,
    rename_step,
    slug_target,
    variable_ids_in_patch,
)

UUID = "01a139ce-1a6c-7753-99e9-6e82d73686c0"
DATASET = "data://grapher/oecd/2026-10-07/climate_related_official_development_assistance"
VAR_PREFIX = "grapher/oecd/2026-10-07/climate_related_official_development_assistance/"

CONFIG_YAML = f"""\
chart_config_id: {UUID}
grapher_schema: "011"
dimensions: []
views:
  - dimensions: {{}}
    indicators:
      y:
        # The comment above the first indicator must survive.
        - catalogPath: "climate_related_oda_by_donor#non_climate"
          display:
            name: "Non-climate-related"
        - catalogPath: "climate_related_oda_by_donor#climate_principal_objective"
          display:
            name: "Climate as main objective"
    config:
      title: "Foreign aid given for climate"
      note: "Old note."
      map:
        colorScale:
          baseColorScheme: "YlGn"
"""

DAG_YAML = f"""\
steps:
  # Climate-related ODA charts — authored in ETL.
  viz://chart/oecd/latest/share_of_foreign_aid_given_that_is_climate_related:
    - {DATASET}
"""

VARIABLE_PATHS = {
    1: VAR_PREFIX + "climate_related_oda_by_donor#non_climate",
    2: VAR_PREFIX + "climate_related_oda_by_donor#climate_principal_objective",
    3: VAR_PREFIX + "climate_related_oda_by_donor#climate_significant_objective",
    4: "grapher/other/2024-01-01/other_dataset/some_table#some_column",
    # Same table#column as an indicator in the YAML, but in an unrelated dataset.
    5: "grapher/other/2024-01-01/other_dataset/climate_related_oda_by_donor#non_climate",
    # The YAML's indicator in an older version of the step's dataset.
    6: "grapher/oecd/2025-01-01/climate_related_official_development_assistance/climate_related_oda_by_donor#non_climate",
}


@pytest.fixture
def repo(tmp_path):
    """A minimal repo with one ETL-authored chart and its DAG entry."""
    step_dir = tmp_path / "steps"
    chart_dir = step_dir / "viz" / "chart" / "oecd" / "latest"
    chart_dir.mkdir(parents=True)
    (chart_dir / "share_of_foreign_aid_given_that_is_climate_related.config.yml").write_text(CONFIG_YAML)
    # An mdim without chart_config_id must not be picked up.
    (chart_dir / "some_mdim.config.yml").write_text("dimensions: []\nviews: []\n")
    dag_dir = tmp_path / "dag"
    dag_dir.mkdir()
    (dag_dir / "main.yml").write_text(DAG_YAML)
    return step_dir, dag_dir


def _step(repo):
    step_dir, dag_dir = repo
    (step,) = find_chart_steps(step_dir, dag_dir)
    return step


def _reconcile(step, patch, skip_fields=(), edited_after_adoption=None, pull_leftovers=False):
    config = load_config(step.config_path)
    layers = ChartLayers(
        chart_id=9287, patch=patch, slug=patch.get("slug"), edited_after_adoption=edited_after_adoption
    )
    layers.variable_paths = {i: VARIABLE_PATHS.get(i) for i in variable_ids_in_patch(patch)}
    result = Result(step=step)
    reconcile_fields(step, config, layers, result, skip_fields, pull_leftovers)
    return config, result


def test_finds_single_charts_with_their_dag_entry(repo):
    step = _step(repo)
    assert step.uri == "viz://chart/oecd/latest/share_of_foreign_aid_given_that_is_climate_related"
    assert step.chart_config_id == UUID
    assert step.dependencies == {DATASET}
    assert step.dag_file is not None and step.dag_file.name == "main.yml"


@pytest.mark.parametrize(
    "dataset, expected",
    [
        ("oecd/2026-10-07/climate_related_official_development_assistance", True),
        ("oecd/climate_related_official_development_assistance", True),
        (DATASET, True),
        ("oecd/2025-01-01/climate_related_official_development_assistance", False),
        ("oecd/official_development_assistance", False),
    ],
)
def test_selects_charts_by_dataset(repo, dataset, expected):
    assert depends_on_dataset(_step(repo), dataset) is expected


def test_identity_keys_are_not_drift():
    patch = {"$schema": "x", "id": 1, "slug": "s", "version": 3, "isPublished": True, "title": "T"}
    assert field_overrides(patch) == {"title": "T"}


def test_pulls_scalars_and_merges_objects(repo):
    patch = {
        "slug": "share-of-foreign-aid-given-that-is-climate-related",
        "note": "New note.",
        "hideTimeline": True,
        "map": {"colorScale": {"binningStrategy": "manual"}},
    }
    config, result = _reconcile(_step(repo), patch)
    view_config = config["views"][0]["config"]
    assert view_config["note"] == "New note."
    assert view_config["hideTimeline"] is True
    # Nested objects merge: the YAML's colorScale keeps its own keys.
    assert dict(view_config["map"]["colorScale"]) == {"baseColorScheme": "YlGn", "binningStrategy": "manual"}
    assert result.pulled == ["note", "hideTimeline", "map"]
    # Only `note` replaced a value the YAML set; the other two only added keys.
    assert result.overrides_etl == ["note"]


def test_dimensions_replace_indicators_in_admin_order(repo):
    patch = {
        "dimensions": [
            {"property": "y", "variableId": 2, "display": {"name": "Main objective"}},
            {"property": "y", "variableId": 3},
            {"property": "y", "variableId": 1, "display": {"name": "Non-climate-related"}},
        ]
    }
    config, result = _reconcile(_step(repo), patch)
    y = config["views"][0]["indicators"]["y"]
    assert [e if isinstance(e, str) else e["catalogPath"] for e in y] == [
        "climate_related_oda_by_donor#climate_principal_objective",
        # Not in the YAML yet: named relative to the step's only grapher dependency.
        "climate_related_oda_by_donor#climate_significant_objective",
        "climate_related_oda_by_donor#non_climate",
    ]
    assert y[0]["display"] == {"name": "Main objective"}
    assert result.pulled == ["dimensions"]
    assert result.overrides_etl == ["dimensions"]


def test_column_slugs_are_mapped_back_to_paths(repo):
    patch = {"sortColumnSlug": "2", "map": {"columnSlug": "1"}}
    config, result = _reconcile(_step(repo), patch)
    view_config = config["views"][0]["config"]
    assert view_config["sortColumnSlug"] == "climate_related_oda_by_donor#climate_principal_objective"
    assert view_config["map"]["columnSlug"] == "climate_related_oda_by_donor#non_climate"
    assert not result.needs_manual


def test_explorer_variable_id_fields_need_manual_fix(repo):
    # Not grapher chart config fields: writing them would fail the schema check on push.
    patch = {"colorVariableId": 3, "xVariableId": "2", "title": "New title"}
    config, result = _reconcile(_step(repo), patch)
    view_config = config["views"][0]["config"]
    assert "colorVariableId" not in view_config and "xVariableId" not in view_config
    assert result.pulled == ["title"]
    assert [m.split(":")[0] for m in result.needs_manual] == ["colorVariableId", "xVariableId"]


def test_values_already_in_the_yaml_are_reported_not_rewritten(repo):
    # E.g. right after the YAML adopted these values, before the next push re-diffs the admin layer.
    patch = {
        "title": "Foreign aid given for climate",
        "dimensions": [
            {"property": "y", "variableId": 1, "display": {"name": "Non-climate-related"}},
            {"property": "y", "variableId": 2, "display": {"name": "Climate as main objective"}},
        ],
    }
    step = _step(repo)
    before = step.config_path.read_text()
    config, result = _reconcile(step, patch)
    assert result.pulled == []
    assert result.already_in_etl == ["title", "dimensions"]
    assert dump_config(config) == before


def test_indicator_from_an_unrelated_dataset_needs_manual_fix(repo):
    patch = {"dimensions": [{"property": "y", "variableId": 4}], "title": "New title"}
    config, result = _reconcile(_step(repo), patch)
    assert result.pulled == ["title"]
    assert len(result.needs_manual) == 1 and "doesn't depend on" in result.needs_manual[0]
    # The indicators were left alone.
    assert len(config["views"][0]["indicators"]["y"]) == 2


def test_same_column_in_an_unrelated_dataset_needs_manual_fix(repo):
    patch = {"sortColumnSlug": "5"}
    config, result = _reconcile(_step(repo), patch)
    assert result.pulled == [] and result.already_in_etl == []
    assert len(result.needs_manual) == 1 and "doesn't depend on" in result.needs_manual[0]
    assert "sortColumnSlug" not in config["views"][0]["config"]


def test_pin_to_an_older_version_of_a_yaml_indicator_is_stale(repo):
    # The admin still points at the old variable; the YAML's path resolves to the current one,
    # so copying would change nothing and the pin would never clear.
    patch = {"sortColumnSlug": "6", "map": {"columnSlug": "6", "colorScale": {"binningStrategy": "manual"}}}
    config, result = _reconcile(_step(repo), patch)
    view_config = config["views"][0]["config"]
    assert "sortColumnSlug" not in view_config
    assert "columnSlug" not in view_config["map"]
    # The rest of the object is still copied.
    assert view_config["map"]["colorScale"]["binningStrategy"] == "manual"
    assert result.stale_pins == ["sortColumnSlug", "map.columnSlug"]
    assert result.pulled == ["map"]
    assert "stale variable pins" in result.status
    assert not result.needs_manual


def test_dimensions_on_an_older_dataset_version_are_stale(repo):
    # Same indicators, order and display as the YAML, only on an older version: nothing to write.
    patch = {
        "dimensions": [
            {"property": "y", "variableId": 6, "display": {"name": "Non-climate-related"}},
            {"property": "y", "variableId": 2, "display": {"name": "Climate as main objective"}},
        ]
    }
    step = _step(repo)
    before = step.config_path.read_text()
    config, result = _reconcile(step, patch)
    assert result.stale_pins == ["dimensions"]
    assert result.pulled == [] and result.already_in_etl == []
    assert dump_config(config) == before


def test_stale_dimensions_with_other_admin_edits_keep_those_edits(repo):
    # An old id plus a new display name and an added indicator: the edits are copied, and the
    # pin is still reported because the admin layer keeps the old id until it is cleared.
    patch = {
        "dimensions": [
            {"property": "y", "variableId": 6, "display": {"name": "Not for climate"}},
            {"property": "y", "variableId": 2, "display": {"name": "Climate as main objective"}},
            {"property": "y", "variableId": 3},
        ]
    }
    config, result = _reconcile(_step(repo), patch)
    y = config["views"][0]["indicators"]["y"]
    assert [_entry_path(e) for e in y] == [
        "climate_related_oda_by_donor#non_climate",
        "climate_related_oda_by_donor#climate_principal_objective",
        "climate_related_oda_by_donor#climate_significant_objective",
    ]
    assert y[0]["display"]["name"] == "Not for climate"
    assert result.stale_pins == ["dimensions"]
    assert result.pulled == ["dimensions"]
    assert result.overrides_etl == ["dimensions"]


def test_stale_dimensions_with_other_edits_are_leftovers_when_nobody_edited(repo):
    patch = {"dimensions": [{"property": "y", "variableId": 6}, {"property": "y", "variableId": 2}]}
    config, result = _reconcile(_step(repo), patch, edited_after_adoption=False)
    assert result.stale_pins == ["dimensions"]
    assert result.leftovers == ["dimensions"]
    assert config["views"][0]["indicators"]["y"][0]["display"]["name"] == "Non-climate-related"


def _entry_path(entry):
    return entry if isinstance(entry, str) else entry["catalogPath"]


def test_skipped_fields_are_left_out(repo):
    config, result = _reconcile(
        _step(repo), {"title": "Stale admin title", "note": "New note."}, skip_fields=("title",)
    )
    assert config["views"][0]["config"]["title"] == "Foreign aid given for climate"
    assert result.pulled == ["note"]
    assert result.skipped == ["title"]


LEFTOVER_PATCH = {
    "title": "Old admin title",
    "hideTimeline": True,
    "dimensions": [
        {"property": "y", "variableId": 1, "display": {"name": "Old name"}},
        {"property": "y", "variableId": 2},
    ],
}


def test_admin_values_from_before_the_move_to_etl_are_left_out(repo):
    # Nobody saved the chart since it moved to ETL: values that differ from the YAML are leftovers.
    config, result = _reconcile(_step(repo), LEFTOVER_PATCH, edited_after_adoption=False)
    view = config["views"][0]
    assert view["config"]["title"] == "Foreign aid given for climate"
    assert view["indicators"]["y"][0]["display"]["name"] == "Non-climate-related"
    assert result.leftovers == ["title", "dimensions"]
    # A value the YAML didn't set masks nothing, so it is still copied.
    assert result.pulled == ["hideTimeline"]
    assert "admin leftovers" in result.status


def test_leftover_inside_an_object_keeps_its_admin_only_keys(repo):
    # `map.colorScale.baseColorScheme` replaces the YAML's value; the other map keys are admin-only.
    patch = {
        "map": {"colorScale": {"baseColorScheme": "Reds", "binningStrategy": "manual"}, "columnSlug": "1", "time": 2020}
    }
    config, result = _reconcile(_step(repo), patch, edited_after_adoption=False)
    map_config = config["views"][0]["config"]["map"]
    assert map_config["colorScale"]["baseColorScheme"] == "YlGn"
    assert map_config["colorScale"]["binningStrategy"] == "manual"
    assert map_config["columnSlug"] == "climate_related_oda_by_donor#non_climate"
    assert map_config["time"] == 2020
    assert result.leftovers == ["map.colorScale.baseColorScheme"]
    assert result.pulled == ["map"]
    assert result.overrides_etl == []


def test_admin_values_edited_after_the_move_are_pulled(repo):
    config, result = _reconcile(_step(repo), LEFTOVER_PATCH, edited_after_adoption=True)
    assert config["views"][0]["config"]["title"] == "Old admin title"
    assert result.leftovers == []
    assert result.pulled == ["title", "hideTimeline", "dimensions"]


@pytest.mark.parametrize("edited_after_adoption, pull_leftovers", [(None, False), (False, True)])
def test_leftovers_are_pulled_when_unknown_or_asked_for(repo, edited_after_adoption, pull_leftovers):
    _, result = _reconcile(
        _step(repo), LEFTOVER_PATCH, edited_after_adoption=edited_after_adoption, pull_leftovers=pull_leftovers
    )
    assert result.leftovers == []
    assert result.overrides_etl == ["title", "dimensions"]


def test_comments_and_key_order_survive(repo):
    config, _ = _reconcile(_step(repo), {"note": "New note."})
    text = dump_config(config)
    assert "# The comment above the first indicator must survive." in text
    assert text.splitlines()[0] == f"chart_config_id: {UUID}"
    assert 'note: "New note."' in text


def test_slug_drift_maps_admin_slug_to_short_name(repo):
    step = _step(repo)
    layers = ChartLayers(chart_id=9287, patch={"slug": "foreign-aid-given-climate"}, slug="foreign-aid-given-climate")
    assert slug_target(step, layers) == ("foreign_aid_given_climate", None)
    in_sync = ChartLayers(chart_id=9287, patch={"slug": step.slug}, slug=step.slug)
    assert slug_target(step, in_sync) == (None, None)


def test_admin_slug_that_cannot_round_trip_needs_manual_rename(repo):
    step = _step(repo)
    layers = ChartLayers(chart_id=9287, patch={"slug": "foreign_aid"}, slug="foreign_aid")
    new_short_name, problem = slug_target(step, layers)
    assert problem is not None and "rename by hand" in problem


def test_rename_step_moves_files_and_dag_entry(repo):
    step_dir, dag_dir = repo
    step = _step(repo)
    step.config_path.with_name(f"{step.short_name}.py").write_text("# code\n")

    rename_step(step, "foreign_aid_given_climate", dag_dir)

    folder = step.config_path.parent
    assert (folder / "foreign_aid_given_climate.config.yml").read_text() == CONFIG_YAML
    assert (folder / "foreign_aid_given_climate.py").exists()
    assert not step.config_path.exists()
    dag_text = (dag_dir / "main.yml").read_text()
    assert "viz://chart/oecd/latest/foreign_aid_given_climate:" in dag_text
    assert "share_of_foreign_aid_given_that_is_climate_related" not in dag_text
    assert "# Climate-related ODA charts — authored in ETL." in dag_text
    # The renamed step is still found, with the same identity.
    (renamed,) = find_chart_steps(step_dir, dag_dir)
    assert renamed.short_name == "foreign_aid_given_climate" and renamed.chart_config_id == UUID


def test_rename_step_refuses_to_clobber(repo):
    _, dag_dir = repo
    step = _step(repo)
    taken = step.config_path.with_name("foreign_aid_given_climate.config.yml")
    taken.write_text("chart_config_id: other\n")
    with pytest.raises(FileExistsError, match="already taken"):
        rename_step(step, "foreign_aid_given_climate", dag_dir)
    # Nothing moved.
    assert step.config_path.exists()
    assert "share_of_foreign_aid_given_that_is_climate_related" in (dag_dir / "main.yml").read_text()


def test_adopting_an_admin_only_chart_fills_an_empty_view(repo):
    step = _step(repo)
    step.config_path.write_text(f"chart_config_id: {UUID}\ndimensions: []\nviews:\n  - dimensions: {{}}\n")
    patch = {"title": "From the admin", "dimensions": [{"property": "y", "variableId": 1}]}
    config, result = _reconcile(step, patch)
    view = config["views"][0]
    assert view["indicators"]["y"] == ["climate_related_oda_by_donor#non_climate"]
    assert view["config"]["title"] == "From the admin"
    assert result.pulled == ["title", "dimensions"]
    # Nothing was set in the YAML, so nothing ETL-authored is being replaced.
    assert result.overrides_etl == []


def _repo_with_superseded_copy(repo):
    """Add an older version of the chart's config, still on disk but no longer in the DAG."""
    step_dir, dag_dir = repo
    old_dir = step_dir / "viz" / "chart" / "oecd" / "2025-01-01"
    old_dir.mkdir(parents=True)
    (old_dir / "share_of_foreign_aid_given_that_is_climate_related.config.yml").write_text(CONFIG_YAML)
    return find_chart_steps(step_dir, dag_dir)


def test_all_and_dataset_select_only_steps_in_the_active_dag(repo):
    steps = _repo_with_superseded_copy(repo)
    assert len(steps) == 2
    active = "viz://chart/oecd/latest/share_of_foreign_aid_given_that_is_climate_related"
    assert [s.uri for s in _select(steps, True, None, ())] == [active]
    assert [s.uri for s in _select(steps, False, "oecd/climate_related_official_development_assistance", ())] == [
        active
    ]
    # Looking a chart up by its id lands on the active step, not the superseded copy.
    found = _step_for_config_id(steps, UUID)
    assert found is not None and found.uri == active


def test_explicit_target_outside_the_dag_is_reconciled_with_a_warning(repo):
    steps = _repo_with_superseded_copy(repo)
    old_uri = "viz://chart/oecd/2025-01-01/share_of_foreign_aid_given_that_is_climate_related"
    (step,) = _select(steps, False, None, (old_uri,))
    assert step.dag_file is None
    layers = ChartLayers(chart_id=9287, patch={"slug": step.slug}, slug=step.slug)
    env = SimpleNamespace(admin_site="http://localhost/admin")
    result = _reconcile_step(step, layers, env, False, True, (), False)
    assert any("isn't in the active DAG" in w for w in result.warnings)


def test_cli_requires_a_selection():
    result = CliRunner().invoke(cli, [])
    assert result.exit_code != 0
    assert "Name the charts" in result.output


def test_patch_without_indicator_references_needs_no_lookup():
    assert variable_ids_in_patch({"title": "T", "map": {"colorScale": {}}}) == set()
