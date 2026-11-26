"""Democracy data explorer, as a multidim. It replaces the `viz://explorer/democracy/latest/democracy` step.

Each per-source `democracy.<source>.config.yml` carries the `metric` and `sub_metric` dimensions
and the views of one source dataset. `combine_charts` adds the `dataset` dimension: each source's
`short_name` becomes its choice slug, with the display names in `SOURCES`.

Indicator FAUST and `display.*` live in the indicators' garden `.meta.yml`, which grapher inherits,
so most views are just dimensions plus `indicators.y`.
"""

import heapq

from etl.helpers import PathFinder
from etl.viz import combine_charts
from etl.viz.chart.explorer_text import add_explorer_text
from etl.viz.chart.model.dimension import DimensionPresentation

paths = PathFinder(__file__)

# Source key (file name and `dataset` choice slug) -> display name, in dropdown order.
SOURCES = {
    "vdem": "Varieties of Democracy",
    "row": "Regimes of the World",
    "lexical_index": "Lexical Index",
    "fh": "Freedom House",
    "bti": "Bertelsmann Transformation Index",
    "eiu": "Economist Intelligence Unit",
    "polity": "Polity",
}


def color_variable_to_indicator(config: dict) -> dict:
    """Move the explorer's `colorVariableId` (in common views or in a view) to the view's color indicator.

    `colorVariableId` is explorer grammar; a multidim view takes it as `indicators.color`.
    """
    shared = None
    for common in config.get("definitions", {}).get("common_views", []) or []:
        common_config = common.get("config") or {}
        if "colorVariableId" in common_config:
            assert "dimensions" not in common, "Only a shared color for all views is supported."
            shared = common_config.pop("colorVariableId")
    for view in config.get("views", []):
        color = (view.get("config") or {}).pop("colorVariableId", None) or shared
        if color:
            view["indicators"]["color"] = color
    return config


def categorical_bins_to_color_scale(config: dict) -> dict:
    """Turn the explorer's `display.colorScaleCategoricalBins` into the view's chart and map color scales.

    `colorScaleCategoricalBins` ("value,color;value,color") is explorer column grammar. Grapher
    reads the same colors from `colorScale` (chart) and `map.colorScale`.
    """
    for view in config.get("views", []):
        for indicator in view.get("indicators", {}).get("y", []):
            if not isinstance(indicator, dict):
                continue
            bins = (indicator.get("display") or {}).pop("colorScaleCategoricalBins", None)
            if not bins:
                continue
            colors = dict(b.split(",")[:2] for b in bins.split(";") if b)
            color_scale = {"binningStrategy": "manual", "customCategoryColors": colors}
            view_config = view.setdefault("config", {})
            view_config["colorScale"] = color_scale
            view_config.setdefault("map", {})["colorScale"] = dict(color_scale)
    return config


def order_choices_as_each_source(combined, charts, dimension: str, within: str | None = None) -> None:
    """Order the choices of `dimension` so that every source lists them in its own order.

    A multidim's dropdown shows a dimension's available choices in one global order, but the same
    choice can sit at different positions in different sources. The explorer worked around this
    with invisible characters in the choice names; here the choices get one order that agrees
    with each source (and, if `within` is given, with each choice of `within` in that source):
    a topological sort, with ties broken by the combined order.
    """
    position = {choice.slug: i for i, choice in enumerate(combined.get_dimension(dimension).choices)}
    after: dict[str, set[str]] = {slug: set() for slug in position}
    for chart in charts:
        source_order = {c.slug: i for i, c in enumerate(chart.get_dimension(dimension).choices)}
        groups: dict[str | None, set[str]] = {}
        for view in chart.views:
            groups.setdefault(view.dimensions[within] if within else None, set()).add(view.dimensions[dimension])
        for slugs in groups.values():
            ordered = sorted(slugs, key=source_order.__getitem__)
            for before, slug in zip(ordered, ordered[1:]):
                after[before].add(slug)

    # Kahn's algorithm, taking the earliest available choice (in the combined order) at each step.
    pending = {slug: 0 for slug in position}
    for slugs in after.values():
        for slug in slugs:
            pending[slug] += 1
    ready = [(position[slug], slug) for slug, n in pending.items() if n == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        _, slug = heapq.heappop(ready)
        order.append(slug)
        for nxt in after[slug]:
            pending[nxt] -= 1
            if pending[nxt] == 0:
                heapq.heappush(ready, (position[nxt], nxt))
    assert len(order) == len(position), f"The sources order the `{dimension}` choices inconsistently."

    dim = combined.get_dimension(dimension)
    dim.choices = sorted(dim.choices, key=lambda choice: order.index(choice.slug))


def run() -> None:
    # Build one multidim per source dataset.
    charts = [
        paths.create_chart(
            config=categorical_bins_to_color_scale(
                color_variable_to_indicator(paths.load_config(f"democracy.{slug}.config.yml"))
            ),
            short_name=slug,
        )
        for slug in SOURCES
    ]

    # Combine them, with a `dataset` dimension to pick the source.
    c = combine_charts(
        charts=charts,
        chart_name=paths.short_name,
        config=paths.load_config(),
        force_chart_dimension=True,
        chart_dimension_slug="dataset",
        chart_dimension_name="Dataset",
        chart_choices_names=list(SOURCES.values()),
    )
    c.get_dimension("dataset").presentation = DimensionPresentation(type="dropdown")

    # Keep each source's own order of metrics, and of sub-metrics within each metric.
    order_choices_as_each_source(c, charts, "metric")
    order_choices_as_each_source(c, charts, "sub_metric", within="metric")

    # Settings every view shares (the explorer set them once, for all views).
    c.set_global_config(
        {
            "hideAnnotationFieldsInTitle": {"entity": True, "time": True, "changeInPrefix": True},
            "originUrl": "https://ourworldindata.org/democracy",
        }
    )

    # Show the text the explorer shows below the chart, until multidims can show their own.
    add_explorer_text(c, paths)

    c.save()
