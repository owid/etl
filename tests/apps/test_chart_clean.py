"""Tests for `etl chart-clean` (apps.chart_clean).

The database reads and the admin writes aren't covered here (they need a grapher server). What's
tested is what decides the cleanup: which admin fields each rule flags, how they are removed, and the
guards around writing.
"""

import pytest
from click.exceptions import UsageError
from click.testing import CliRunner

from apps.chart_clean.cli import cli, dataset_path_pattern, staging_env
from apps.chart_clean.rules import find_noop_fields, remove_paths


def _full(chart_types=("LineChart",), dims=((1, "y"),), **extra):
    """A rendered chart config with the given chart types and (variableId, property) dimensions."""
    return {
        "chartTypes": list(chart_types),
        "dimensions": [{"variableId": v, "property": p} for v, p in dims],
        **extra,
    }


def _paths(full, patch):
    return [f.path for f in find_noop_fields(full, patch)]


def test_map_column_repeating_the_first_y_is_redundant():
    assert _paths(_full(), {"map": {"columnSlug": "1"}}) == ["map.columnSlug"]


def test_map_column_outside_the_dimensions_is_stale():
    findings = find_noop_fields(_full(), {"map": {"columnSlug": "999"}})
    assert [(f.path, f.rule) for f in findings] == [("map.columnSlug", "stale map column")]


def test_map_column_naming_another_y_is_kept():
    # It picks which indicator the map shows, and formats values in search results and downloads.
    assert _paths(_full(dims=((1, "y"), (2, "y"))), {"map": {"columnSlug": "2"}}) == []


def test_map_column_matches_target_year_slugs():
    full = {"chartTypes": ["ScatterPlot"], "dimensions": [{"variableId": 1, "property": "y", "targetYear": 2000}]}
    assert _paths(full, {"map": {"columnSlug": "1-2000"}}) == ["map.columnSlug"]


@pytest.mark.parametrize(
    "chart_types, sort_by, expected",
    [
        (("StackedDiscreteBar",), "column", []),
        (("Dumbbell",), "column", []),
        (("StackedDiscreteBar",), "total", ["sortColumnSlug"]),
        (("DiscreteBar",), "column", ["sortColumnSlug"]),
    ],
)
def test_sort_column_only_counts_when_sorting_by_a_column(chart_types, sort_by, expected):
    full = _full(chart_types=chart_types, sortBy=sort_by)
    assert _paths(full, {"sortColumnSlug": "1"}) == expected


def test_sort_column_outside_the_dimensions_is_stale():
    full = _full(chart_types=("StackedDiscreteBar",), sortBy="column")
    assert _paths(full, {"sortColumnSlug": "999"}) == ["sortColumnSlug"]


@pytest.mark.parametrize(
    "chart_types, dims, expected",
    [
        (("LineChart",), ((1, "y"),), ["colorScale"]),  # no color dimension
        (("ScatterPlot",), ((1, "y"), (2, "x"), (3, "color")), []),
        (("StackedArea",), ((1, "y"), (3, "color")), ["colorScale"]),  # stacked charts never read it
    ],
)
def test_color_scale_needs_a_color_dimension_and_a_reading_chart_type(chart_types, dims, expected):
    assert _paths(_full(chart_types=chart_types, dims=dims), {"colorScale": {"baseColorScheme": "Oranges"}}) == expected


def test_default_chart_types_include_discrete_bar():
    # Without `chartTypes`, grapher shows LineChart and DiscreteBar, so DiscreteBar-only fields stay.
    full = {"dimensions": [{"variableId": 1, "property": "y"}]}
    assert _paths(full, {"showYearLabels": True, "zoomToSelection": True}) == ["zoomToSelection"]


def test_scatter_only_fields_on_a_line_chart():
    patch = {"zoomToSelection": True, "hideConnectedScatterLines": True, "stackMode": "relative"}
    # stackMode is read by line charts, so it stays.
    assert _paths(_full(), patch) == ["zoomToSelection", "hideConnectedScatterLines"]


def test_any_listed_chart_type_keeps_a_field():
    patch = {"zoomToSelection": True}
    assert _paths(_full(chart_types=("LineChart", "ScatterPlot")), patch) == []


def test_map_only_chart_ignores_chart_type_fields():
    full = _full(chart_types=(), hasMapTab=True)
    assert _paths(full, {"hideRelativeToggle": False, "map": {"time": 2020}}) == ["hideRelativeToggle"]


def test_map_tab_settings_without_a_map_tab():
    patch = {
        "map": {"time": 2020, "timeTolerance": 5, "selectedEntityNames": ["France"], "tooltipUseCustomLabels": True}
    }
    # Selection and custom labels are still read without a map tab (table selection, value labels).
    assert _paths(_full(hasMapTab=False), patch) == ["map.time", "map.timeTolerance"]
    assert _paths(_full(hasMapTab=True), patch) == []


def test_fields_outside_the_admin_patch_are_never_flagged():
    full = _full(colorScale={"baseColorScheme": "Oranges"}, zoomToSelection=True)
    assert _paths(full, {}) == []


def test_remove_paths_drops_nested_keys_and_empty_objects():
    config = {"title": "T", "map": {"columnSlug": "1", "time": 2020}, "colorScale": {"x": 1}}
    cleaned = remove_paths(config, ["map.columnSlug", "map.time", "colorScale"])
    assert cleaned == {"title": "T"}
    # The input is left as it was.
    assert config["map"] == {"columnSlug": "1", "time": 2020}


def test_remove_paths_keeps_the_rest_of_an_object():
    cleaned = remove_paths({"map": {"columnSlug": "1", "colorScale": {"a": 1}}}, ["map.columnSlug"])
    assert cleaned == {"map": {"colorScale": {"a": 1}}}


@pytest.mark.parametrize(
    "dataset, pattern",
    [
        # `_` is a LIKE wildcard, so it is escaped (with the query's ESCAPE character `!`) to match literally.
        ("demography/2025-10-22/efr_malani_jacob", "grapher/demography/2025-10-22/efr!_malani!_jacob/%"),
        ("demography/efr_malani_jacob", "grapher/demography/%/efr!_malani!_jacob/%"),
        ("data://grapher/demography/efr_malani_jacob", "grapher/demography/%/efr!_malani!_jacob/%"),
        ("ns/a%b!c", "grapher/ns/%/a!%b!!c/%"),
    ],
)
def test_dataset_selection_pattern(dataset, pattern):
    assert dataset_path_pattern(dataset) == pattern


@pytest.mark.parametrize("branch", ["master", "main", ""])
def test_apply_refuses_master(branch):
    with pytest.raises(UsageError, match="never master or production"):
        staging_env(branch)


def test_cli_requires_a_selection():
    result = CliRunner().invoke(cli, [])
    assert result.exit_code != 0
    assert "Name the charts" in result.output


def test_remove_paths_handles_deep_paths():
    config = {"map": {"colorScale": {"binningStrategy": "manual", "baseColorScheme": "x"}}}
    assert remove_paths(config, ["map.colorScale.binningStrategy"]) == {"map": {"colorScale": {"baseColorScheme": "x"}}}
    assert remove_paths(config, ["map.colorScale.binningStrategy", "map.colorScale.baseColorScheme"]) == {}
