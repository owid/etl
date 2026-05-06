"""Build the food-footprints explorer ("Environmental Impacts of Food").

Hybrid migration: the "Commodity" half of the explorer (16 views) draws on two existing
ETL grapher datasets — Poore & Nemecek (2018) "Environmental impacts of food" and the
companion life-cycle-stage table — while the "Specific food products" half (29 views)
draws on the freshly-migrated Clark et al. (2022) dataset (food/2026-05-06).

All chart text and per-view config live in this YAML; nothing flows from indicator
metadata, so this step is intentionally minimal.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    config = paths.load_collection_config()
    c = paths.create_collection(
        config=config,
        short_name="food-footprints",
        explorer=True,
    )
    c.save(tolerate_extra_indicators=True)
