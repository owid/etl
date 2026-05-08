"""Build the Democracy explorer by combining one mini-explorer per upstream source.

Each per-source `democracy.<source>.config.yml` carries its own views and the
dim-choices subset they touch (in canonical order). Indicator FAUST and
display.* live in the indicator's garden meta.yml — Grapher inherits both at
chart render time, so most views are just `dimensions` + `indicators.y[catalogPath]`.

There is no top-level `democracy.config.yml` — the explorer settings live in
`TOP_CONFIG` below, and the canonical dim order is implied by the order in
which sub-configs are loaded (each sub-config lists its choices in canonical
order; `combine_collections` takes the union in first-seen order, so loading
sources in dataset order yields the canonical sequence).
"""

from etl.collection import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Order matters: the combined explorer's dim choices come out in the order
# they first appear across these sub-configs. `vdem` covers both
# `varieties_of_democracy` and `regimes_of_the_world` dataset choices.
SOURCES = ["vdem", "lexical_index", "fh", "bti", "eiu", "polity"]


# Top-level explorer settings (formerly democracy.config.yml).
TOP_CONFIG = {
    "explorerTitle": "Democracy",
    "explorerSubtitle": "Explore changes in the world's democratic and non-democratic systems.",
    "isPublished": True,
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
            config=paths.load_collection_config(f"democracy.{src}.config.yml"),
            short_name=f"democracy_{src}",
            explorer=True,
        )
        for src in SOURCES
    ]

    final = combine_collections(
        collections=explorers,
        collection_name="democracy",
        config={"config": TOP_CONFIG},
    )

    final.save(tolerate_extra_indicators=True)
