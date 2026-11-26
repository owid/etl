"""Democracy data explorer, as a multidim. It replaces the `viz://explorer/democracy/latest/democracy` step.

Each per-source `democracy.<source>.config.yml` carries the `metric` and `sub_metric` dimensions
and the views of one source dataset. `combine_charts` adds the `dataset` dimension: each source's
`short_name` becomes its choice slug, with the display names in `SOURCES`.

Indicator FAUST and `display.*` live in the indicators' garden `.meta.yml`, which grapher inherits,
so most views are just dimensions plus `indicators.y`.
"""

from etl.helpers import PathFinder
from etl.viz import combine_charts
from etl.viz.chart.explorer_text import add_explorer_text

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


def run() -> None:
    # Build one multidim per source dataset.
    charts = [
        paths.create_chart(config=paths.load_config(f"democracy.{slug}.config.yml"), short_name=slug)
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
