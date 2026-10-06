"""Bespoke viz step producing the JSON feed for the food supply chain waterfall visualization.

Publishes the method of its garden dependency in the DAG (the Supply Utilization Accounts); see `shared.py`.
"""

from shared import build_feed

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    build_feed(paths)
