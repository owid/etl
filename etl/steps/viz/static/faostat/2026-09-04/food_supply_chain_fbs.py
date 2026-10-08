"""Static viz step writing the data of the food supply chain waterfall of the world in 1968, Food Balance Sheets method.

The same files as the bespoke step `food_supply_chain_scl`, built from the FBS garden dataset and restricted to the one
country and year of the static chart, so that the interactive waterfall can draw it. Nothing is uploaded; the files stay
in the step's local output folder. See `shared.py` in the bespoke step's folder.
"""

from etl.helpers import PathFinder
from etl.steps.viz.bespoke.faostat.latest.shared import build_feed

paths = PathFinder(__file__)

# Country and year of the static chart (1968 is the year The Population Bomb was published).
COUNTRY = "World"
YEAR = 1968
# Stages left out of the chart: tourist consumption is set to zero for the world, and data adjustments are about 1% of
# food, less than the rounding of the chart's labels.
NEGLIGIBLE_STAGES_TO_DROP = ["tourist_consumption", "data_adjustments"]


def run() -> None:
    build_feed(paths, countries=[COUNTRY], years=[YEAR], negligible_stages_to_drop=NEGLIGIBLE_STAGES_TO_DROP)
