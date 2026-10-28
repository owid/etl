"""Rules behind `etl chart-clean`: admin fields that have no effect on how a chart renders.

Every rule reads a chart's configs only, never its data, and flags a field only when no chart type the
chart can show reads it. Grapher may drop some configured chart types depending on the data
(`GrapherState.validChartTypes`), so `chartTypes` is treated as the widest possible set: a field read by
any listed type is kept. Every rule cites the owid-grapher code (packages/@ourworldindata/grapher/src/)
that makes the field a no-op.

Only fields set in the admin patch (`charts.patchConfigId`) are flagged; ETL and indicator configs are
never touched.
"""

import copy
from dataclasses import dataclass
from typing import Any

# Chart types grapher assumes when a config doesn't set `chartTypes` (schema default, defaultGrapherConfig.ts).
DEFAULT_CHART_TYPES = ("LineChart", "DiscreteBar")

# Top-level fields read only by some chart types, and the types that read them. A field set on a chart
# none of whose types reads it does nothing.
FIELD_READERS: dict[str, frozenset[str]] = {
    # ScatterPlotChartState / ScatterPlotChart.
    "zoomToSelection": frozenset({"ScatterPlot"}),
    "hideConnectedScatterLines": frozenset({"ScatterPlot"}),
    "hideScatterLabels": frozenset({"ScatterPlot"}),
    "scatterPointLabelStrategy": frozenset({"ScatterPlot"}),
    "compareEndPointsOnly": frozenset({"ScatterPlot"}),
    # ScatterPlotChartState:104, MarimekkoChartState:65.
    "matchingEntitiesOnly": frozenset({"ScatterPlot", "Marimekko"}),
    # MarimekkoChartState:82.
    "showNoDataArea": frozenset({"Marimekko"}),
    # StackedDiscreteBarChart:330.
    "hideTotalValueLabel": frozenset({"StackedDiscreteBar"}),
    # DiscreteBarChart:311.
    "showYearLabels": frozenset({"DiscreteBar"}),
    # DumbbellChartState:134-154.
    "dumbbell": frozenset({"Dumbbell"}),
    # GrapherState.isRelativeMode / canToggleRelativeMode: not DiscreteBar, Dumbbell or the map.
    "stackMode": frozenset(
        {"LineChart", "SlopeChart", "ScatterPlot", "Marimekko", "StackedArea", "StackedBar", "StackedDiscreteBar"}
    ),
    "hideRelativeToggle": frozenset(
        {"LineChart", "SlopeChart", "ScatterPlot", "Marimekko", "StackedArea", "StackedBar", "StackedDiscreteBar"}
    ),
    # Chart states that read it: Line, Slope, DiscreteBar, Marimekko, AbstractStacked, StackedDiscreteBar.
    "baseColorScheme": frozenset(
        {"LineChart", "SlopeChart", "DiscreteBar", "Marimekko", "StackedArea", "StackedBar", "StackedDiscreteBar"}
    ),
    "invertColorScheme": frozenset(
        {"LineChart", "SlopeChart", "DiscreteBar", "StackedArea", "StackedBar", "StackedDiscreteBar"}
    ),
    # GrapherState.showSeriesLabels.
    "hideSeriesLabels": frozenset({"LineChart", "SlopeChart", "StackedArea", "ScatterPlot"}),
    # LineChart, ScatterPlotChart, MarimekkoChart, StackedAreaChart, StackedBarChart.
    "comparisonLines": frozenset({"LineChart", "ScatterPlot", "Marimekko", "StackedArea", "StackedBar"}),
    # LineChartState, SlopeChartState, AbstractStackedChartState, StackedDiscreteBarChartState.
    "missingDataStrategy": frozenset({"LineChart", "SlopeChart", "StackedArea", "StackedBar", "StackedDiscreteBar"}),
}

# Chart types that build a ColorScale from the top-level `colorScale`: LineChartState, DiscreteBarChartState,
# ScatterPlotChartState, MarimekkoChartState. They only show it when the chart has a color dimension.
COLOR_SCALE_READERS = frozenset({"LineChart", "DiscreteBar", "ScatterPlot", "Marimekko"})

# Chart types that sort by `sortColumnSlug` (sortBy "column"): StackedDiscreteBarChartState, DumbbellChartState.
# DiscreteBar accepts sortBy "column" but sorts by the bar's own value (DiscreteBarChartState.sortedRawSeries).
SORT_COLUMN_READERS = frozenset({"StackedDiscreteBar", "Dumbbell"})

# `map.*` settings only read on the map tab (GrapherState: map time, tolerance and timeline are
# map-tab-only; `globe` and `region` only drive the map view). Kept without a map tab: `selectedEntityNames`
# (table selection, GrapherState.dataTableSelection), and `tooltipUseCustomLabels`, `colorScale` and
# `columnSlug`, which format values in search results, data callouts and downloads
# (GrapherValuesJson, MapChartState.formatValueForTooltip).
MAP_TAB_ONLY_KEYS = ("time", "startTime", "timeTolerance", "toleranceStrategy", "hideTimeline", "globe", "region")


@dataclass(frozen=True)
class Finding:
    """One admin field that has no effect on the chart."""

    path: str
    rule: str
    reason: str


def find_noop_fields(full: dict[str, Any], patch: dict[str, Any]) -> list[Finding]:
    """Admin-patch fields of a chart that change nothing on screen.

    `full` is the chart's rendered config (what grapher serves), `patch` its admin layer.
    """
    chart_types = set(full.get("chartTypes", DEFAULT_CHART_TYPES))
    has_map_tab = bool(full.get("hasMapTab", False))
    dimensions = full.get("dimensions") or []
    dimension_slugs = {_column_slug(d) for d in dimensions}
    first_y = next((_column_slug(d) for d in dimensions if d.get("property") == "y"), None)
    has_color_dimension = any(d.get("property") == "color" for d in dimensions)
    findings: list[Finding] = []

    map_value = patch.get("map")
    patch_map: dict[str, Any] = map_value if isinstance(map_value, dict) else {}

    # Rule 1: GrapherState.mapColumnSlug falls back to the first y when the slug is absent or not a dimension.
    column_slug = patch_map.get("columnSlug")
    if column_slug is not None:
        if str(column_slug) == first_y:
            findings.append(
                Finding("map.columnSlug", "redundant map column", "it names the first y indicator, the default")
            )
        elif str(column_slug) not in dimension_slugs:
            findings.append(
                Finding("map.columnSlug", "stale map column", "it names an indicator the chart doesn't plot")
            )

    # Rule 2: only StackedDiscreteBar and Dumbbell sort by column, and only with sortBy "column".
    if "sortColumnSlug" in patch:
        if full.get("sortBy") != "column" or not chart_types & SORT_COLUMN_READERS:
            findings.append(Finding("sortColumnSlug", "unused sort column", "the chart doesn't sort by a column"))
        elif str(patch["sortColumnSlug"]) not in dimension_slugs:
            findings.append(
                Finding("sortColumnSlug", "stale sort column", "it names an indicator the chart doesn't plot")
            )

    # Rule 3: a top-level colorScale only shows with a color dimension, on the chart types that build one.
    if "colorScale" in patch and (not has_color_dimension or not chart_types & COLOR_SCALE_READERS):
        reason = "the chart has no color dimension" if not has_color_dimension else "no chart type reads it"
        findings.append(Finding("colorScale", "unused color scale", reason))

    # Rule 4: fields read only by chart types the chart doesn't have.
    for field, readers in FIELD_READERS.items():
        if field in patch and not chart_types & readers:
            findings.append(Finding(field, "field unused by chart type", f"only {', '.join(sorted(readers))} read it"))

    # Rule 5: map-tab-only settings on a chart without a map tab.
    if not has_map_tab:
        for key in MAP_TAB_ONLY_KEYS:
            if key in patch_map:
                findings.append(Finding(f"map.{key}", "map setting without a map tab", "the chart has no map tab"))

    return findings


def remove_paths(config: dict[str, Any], paths: list[str]) -> dict[str, Any]:
    """A copy of `config` without the given dotted paths; an object left empty is dropped too."""
    result = copy.deepcopy(config)
    for path in paths:
        _remove_path(result, path.split("."))
    return result


def keep_paths(config: dict[str, Any], paths: list[str]) -> dict[str, Any]:
    """Only the given dotted paths of `config` (the ones it has), nested as in `config`."""
    result: dict[str, Any] = {}
    for path in paths:
        *parents, last = path.split(".")
        source, target = config, result
        for key in parents:
            source = source.get(key) if isinstance(source, dict) else None
            target = target.setdefault(key, {})
        if isinstance(source, dict) and last in source:
            target[last] = copy.deepcopy(source[last])
    return result


def _remove_path(node: dict[str, Any], keys: list[str]) -> None:
    head, rest = keys[0], keys[1:]
    if not rest:
        node.pop(head, None)
        return
    child = node.get(head)
    if isinstance(child, dict):
        _remove_path(child, rest)
        if not child:
            node.pop(head)


def _column_slug(dimension: dict[str, Any]) -> str:
    """A dimension's column slug, as grapher builds it (ChartDimension.columnSlug)."""
    variable_id = dimension.get("variableId")
    target_year = dimension.get("targetYear")
    return f"{variable_id}-{target_year}" if target_year is not None else str(variable_id)
