"""Build the Democracy explorer by combining one mini-explorer per source dataset.

Each per-source `democracy.<key>.config.yml` carries only `metric` + `sub_metric`
dimensions plus its views. The `dataset` dimension is added automatically by
`combine_charts` via `chart_dimension_slug` — each mini's
`short_name` becomes the dataset choice slug, with display names provided by
`chart_choices_names`.

Indicator FAUST and `display.*` live in the indicator's garden meta.yml —
Grapher inherits them at chart render time, so most views are just
`dimensions.{metric, sub_metric}` + `indicators.y[catalogPath]`.
"""

from etl.helpers import PathFinder
from etl.viz import combine_charts

paths = PathFinder(__file__)


# (yaml_filename_key, dataset_slug, dataset_display_name)
# Order = canonical order of the `Dataset` dropdown choices.
SOURCES = {
    "vdem": "Varieties of Democracy",
    "row": "Regimes of the World",
    "lexical_index": "Lexical Index",
    "fh": "Freedom House",
    "bti": "Bertelsmann Transformation Index",
    "eiu": "Economist Intelligence Unit",
    "polity": "Polity",
}


TOP_CONFIG = {
    "explorerTitle": "Democracy",
    "explorerSubtitle": "Explore changes in the world's democratic and non-democratic systems.",
    "isPublished": True,
    "originUrl": "https://ourworldindata.org/democracy",
    "wpBlockId": "51850",
    "hideAnnotationFieldsInTitle": True,
    "pickerColumnSlugs": [],
    "selection": ["Argentina", "Australia", "Botswana", "China", "World"],
}


def run() -> None:
    explorers = [
        paths.create_explorer(
            config=paths.load_config(f"democracy.{slug}.config.yml"),
            short_name=slug,  # becomes the `dataset` dim choice slug after combine
        )
        for slug in SOURCES.keys()
    ]

    final = combine_charts(
        charts=explorers,
        chart_name="democracy",
        config={"config": TOP_CONFIG},
        force_chart_dimension=True,
        chart_dimension_slug="dataset",
        chart_dimension_name="Dataset",
        chart_choices_names=[name for name in SOURCES.values()],
    )

    final.save(tolerate_extra_indicators=True)
