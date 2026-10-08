"""Static viz step writing the data of the food supply chain waterfall of the world in 1968, Food Balance Sheets method.

The same files as the bespoke step `food_supply_chain_scl`, built from the FBS garden dataset and restricted to the one
country and year of the static chart, so that the interactive waterfall can draw it. Nothing is uploaded; the files stay
in the step's local output folder. See `shared.py` in the bespoke step's folder.

The chart is in the Charts (2026) Figma file, page "20261008 How many calories did the world produce in 1968, and where
did they go? (Pablo R)", frame `world-food-supply-chain-calories-1968` (node 28837:6):
https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=28837-6
It was drawn by the interactive waterfall reading this step's files (served locally to the bespoke dev server of
owid-grapher, with `BESPOKE_DATA_URL`); in Figma, the value labels were rounded to the nearest 100 kcal and "Biofuels
and industry" was relabeled "Industry and other uses".
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
