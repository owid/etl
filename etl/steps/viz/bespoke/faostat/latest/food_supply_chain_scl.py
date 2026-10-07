"""Bespoke viz step writing the JSON files for the food supply chain waterfall, Supply Utilization Accounts method.

Built from the SCL garden dataset, whose chain runs from 2010; the main source of the interactive waterfall. See
`shared.py`.
"""

from shared import build_feed

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    build_feed(paths)
