"""Build the Democracy explorer by combining one mini-explorer per upstream source.

Each per-source `democracy.<source>.config.yml` carries only that source's views
and the dim-choices subset they touch. Indicator FAUST (title, subtitle, note,
map config, etc.) lives in the indicator's `presentation.grapher_config` in the
corresponding garden meta.yml — Grapher inherits it at chart render time, so
single-y views in the per-source YAMLs carry no per-view config.

`combine_collections` unions the dimension choices across the 6 minis. We then
call `sort_choices` to enforce the canonical order from democracy.config.yml.
"""

from etl.collection import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


SOURCES = ["vdem", "lexical_index", "fh", "bti", "eiu", "polity"]


def run() -> None:
    main_config = paths.load_collection_config()

    # Build one mini-explorer per upstream source.
    explorers = []
    for src in SOURCES:
        sub_config = paths.load_collection_config(f"democracy.{src}.config.yml")
        c = paths.create_collection(
            config=sub_config,
            short_name=f"democracy_{src}",
            explorer=True,
        )
        explorers.append(c)

    # Combine into the final explorer (slug "democracy"), passing the canonical
    # config (explorer settings + full dimensions list) as the merge target.
    final = combine_collections(
        collections=explorers,
        collection_name="democracy",
        config=main_config,
    )

    # Enforce canonical dim-choice order (combine_collections takes the union
    # of choices from sub-collections in first-seen order).
    canonical_choice_order = {
        d["slug"]: [c["slug"] for c in d["choices"]]
        for d in main_config["dimensions"]
    }
    final.sort_choices(canonical_choice_order)

    final.save(tolerate_extra_indicators=True)
