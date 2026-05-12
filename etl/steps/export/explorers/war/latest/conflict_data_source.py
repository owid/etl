"""Build the Conflict Data Source explorer by combining one mini-explorer per data source.

Each per-source `conflict_data_source.<key>.config.yml` carries only the four shared
dimensions (`conflict_type`, `measure`, `conflict_sub_type`, `sub_measure`) plus its
views. The `data_source` dimension is added automatically by `combine_collections`
via `collection_dimension_slug` — each mini's `short_name` becomes the `data_source`
choice slug, with display names provided by `collection_choices_names`.

A future sibling explorer (`conflict-data`) will reuse these same sub-configs with a
different leading dropdown (Conflict type first instead of Data source) — keep the
dim slugs neutral so the same building blocks can drive both.
"""

from etl.collection import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


# (short_name → display name). Order = canonical order of the `Data source` dropdown,
# matching the legacy PROD TSV exactly.
SOURCES = {
    "ucdp": "Uppsala Conflict Data Program",
    "ucdp_prio": "UCDP + PRIO",
    "mars": "Project Mars",
    "mie": "Militarized Interstate Events",
    "cow": "Correlates of War – Wars",
    "cow_mid": "Correlates of War — Militarized Interstate Disputes",
    "prio": "Peace Research Institute Oslo",
}


TOP_CONFIG = {
    "explorerTitle": "Conflict Data Source",
    "explorerSubtitle": "Explore the world's conflicts through the leading approaches to measuring them.",
    "isPublished": True,
    "selection": ["World"],
    "subNavId": "explorers",
    "subNavCurrentId": "conflict-data-source",
    "entityType": "region",
    "hideAnnotationFieldsInTitle": True,
    "yAxisMin": 0,
}


def run() -> None:
    explorers = [
        paths.create_collection(
            config=paths.load_collection_config(f"conflict_data_source.{slug}.config.yml"),
            short_name=slug,  # becomes the `data_source` dim choice slug after combine
            explorer=True,
        )
        for slug in SOURCES.keys()
    ]

    final = combine_collections(
        collections=explorers,
        collection_name="conflict-data-source",
        config={"config": TOP_CONFIG},
        force_collection_dimension=True,
        collection_dimension_slug="data_source",
        collection_dimension_name="Data source",
        collection_choices_names=[name for name in SOURCES.values()],
    )

    final.save(tolerate_extra_indicators=True)
