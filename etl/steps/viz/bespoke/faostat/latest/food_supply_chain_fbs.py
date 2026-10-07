"""Bespoke viz step writing the JSON files for the food supply chain waterfall, Food Balance Sheets method.

The same files as `food_supply_chain_scl`, built from the FBS garden dataset, whose chain runs from 1961; made for
the static waterfall of the world in 1968. See `shared.py`.
"""

from shared import build_feed

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    build_feed(paths)
