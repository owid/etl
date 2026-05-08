"""Build the Democracy explorer by combining one mini-explorer per upstream source.

Each per-source mini carries only that source's views and the dim-choices
subset they touch. Indicator FAUST (title, subtitle, note, map config, etc.)
and `display.{name, color, shortUnit, numDecimalPlaces, ...}` live in the
indicator's garden meta.yml — Grapher inherits them at chart render time.

Most sources keep their views in `democracy.<source>.config.yml`. **BTI is a
proof-of-concept for programmatic view construction** (see `democracy_bti.py`)
— this is here so we can compare YAML vs. Python authoring side by side.

`combine_collections` unions the dimension choices across the 6 minis. We then
call `sort_choices` to enforce the canonical order from democracy.config.yml.
"""

from democracy_bti import build_bti

from etl.collection import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


YAML_SOURCES = ["vdem", "lexical_index", "fh", "eiu", "polity"]


def run() -> None:
    main_config = paths.load_collection_config()

    # Pre-compute dim metadata for the programmatic builder (BTI).
    dim_meta = {
        d["slug"]: {
            "name": d["name"],
            "type": d["presentation"]["type"],
            "order": [c["slug"] for c in d["choices"]],
            "choices": {c["slug"]: c["name"] for c in d["choices"]},
        }
        for d in main_config["dimensions"]
    }

    # Build mini-explorers — one per upstream source.
    explorers = []
    for src in YAML_SOURCES:
        sub_config = paths.load_collection_config(f"democracy.{src}.config.yml")
        explorers.append(paths.create_collection(
            config=sub_config,
            short_name=f"democracy_{src}",
            explorer=True,
        ))
    explorers.append(build_bti(paths, dim_meta, main_config["config"]))

    # Combine into the final explorer (slug "democracy").
    final = combine_collections(
        collections=explorers,
        collection_name="democracy",
        config=main_config,
    )

    # Enforce canonical dim-choice order (combine takes union in first-seen order).
    final.sort_choices({slug: meta["order"] for slug, meta in dim_meta.items()})

    final.save(tolerate_extra_indicators=True)
