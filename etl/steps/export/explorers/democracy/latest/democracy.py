"""Build the Democracy explorer by combining one mini-explorer per source dataset.

Each per-source `democracy.<key>.config.yml` carries only `metric` + `sub_metric`
dimensions plus its views. The `dataset` dimension is added automatically by
`combine_collections` via `collection_dimension_slug` — each mini's
`short_name` becomes the dataset choice slug, with display names provided by
`collection_choices_names`.

Indicator FAUST and `display.*` live in the indicator's garden meta.yml —
Grapher inherits them at chart render time, so most views are just
`dimensions.{metric, sub_metric}` + `indicators.y[catalogPath]`.
"""

from etl.collection import combine_collections
from etl.helpers import PathFinder

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
    "thumbnail": "https://assets.ourworldindata.org/uploads/2022/06/Democracy-Data-Explorer.png",
    "wpBlockId": "51850",
    "subNavId": "explorers",
    "subNavCurrentId": "democracy-data",
    "hideAlertBanner": True,
    "hideAnnotationFieldsInTitle": True,
    "pickerColumnSlugs": [],
    "selection": ["Argentina", "Australia", "Botswana", "China", "World"],
}




def run() -> None:
    explorers = [
        paths.create_collection(
            config=paths.load_collection_config(f"democracy.{slug}.config.yml"),
            short_name=slug,  # becomes the `dataset` dim choice slug after combine
            explorer=True,
        )
        for slug in SOURCES.keys()
    ]

    final = combine_collections(
        collections=explorers,
        collection_name="democracy",
        config={"config": TOP_CONFIG},
        force_collection_dimension=True,
        collection_dimension_slug="dataset",
        collection_dimension_name="Dataset",
        collection_choices_names=[name for name in SOURCES.values()],
    )

    final.save(tolerate_extra_indicators=True)
