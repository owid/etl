"""Static viz step writing the data of the food supply chain waterfall of the world in 1968, Food Balance Sheets method.

It writes, from the FBS garden dataset and for the one country and year of the static chart, the same files as the
bespoke step `food_supply_chain_scl`, so that the interactive waterfall can draw it:

  * one metadata JSON at `food-supply-chain.metadata.json`: the method, the sources, the year range, the stages of
    the chain in order (with a label and whether the bar adds to or takes from the chain), the units, the
    entity id-to-name mapping, and the feed's provenance derived from the garden columns (see `etl.viz.bespoke`);
  * one JSON at `food-supply-chain.1.json`, with the year and, for each unit (energy, protein, mass), one value per
    stage.

Reshaping only; all logic lives in the garden step. Values are FAO's sign convention: stages listed with
"direction": "out" are magnitudes to subtract along the chain (a negative value there adds back). Nothing is uploaded;
the files stay in the step's local output folder.

The chart is in the Charts (2026) Figma file, page "20261008 How many calories did the world produce in 1968, and where
did they go? (Pablo R)", frame `world-food-supply-chain-calories-1968` (node 28837:6):
https://www.figma.com/design/s6Sv60bakebRRW2TxsMQbF/Charts--2026-?node-id=28837-6
It was drawn by the interactive waterfall reading this step's files (served locally to the bespoke dev server of
owid-grapher, with `BESPOKE_DATA_URL`), with every font size in the waterfall's `core/constants.ts` raised by 1px and
`VERTICAL_CHART_HEIGHT` raised from 400 to 442 to fill the template (local changes, not committed to owid-grapher). In Figma, the value labels were rounded to the nearest 100 kcal and
"Biofuels and industry" was relabeled "Industry and other uses".
"""

import json

from etl.helpers import PathFinder
from etl.viz.bespoke import add_feed_metadata, build_feed_metadata

paths = PathFinder(__file__)

# Country and year of the static chart (1968 is the year The Population Bomb was published).
COUNTRY = "World"
YEAR = 1968
FILE_SLUG = "food-supply-chain"
# Id of the only entity in the files.
ENTITY_ID = 1
# How the method is named in the metadata.
METHOD = "Food Balance Sheets"
# The three tables of the garden dataset.
NUTRIENTS = ["energy", "protein", "mass"]
# Stages in chain order, with a display label and whether the bar adds to ("in") or takes from ("out") the chain.
# "food" is the total the chain lands on.
STAGES = [
    ("crop_production", "Crop production", "in"),
    ("imports", "Imports", "in"),
    ("exports", "Exports", "out"),
    ("stock_variation", "Stock change", "out"),
    ("seed", "Seed", "out"),
    ("losses", "Losses in the supply chain", "out"),
    ("other_uses", "Industrial and other non-food uses", "out"),
    ("processing_net", "Processing, net", "out"),
    ("feed", "Animal feed", "out"),
    ("animal_products", "Livestock, dairy, eggs and fish", "in"),
    ("food", "Food available to eat", "total"),
]
# Stages left out of the chart: tourist consumption is set to zero for the world, and data adjustments are about 1% of
# food, less than the rounding of the chart's labels.
NEGLIGIBLE_STAGES = ["tourist_consumption", "data_adjustments"]
# Largest size, as a share of food, of a stage that can be left out as negligible.
MAX_NEGLIGIBLE_STAGE_SHARE = 0.02
# Decimal places kept per unit.
NUM_DECIMALS = {"energy": 1, "protein": 2, "mass": 4}


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds = paths.load_dataset("food_supply_chain_fbs")
    tables = {nutrient: ds.read(nutrient) for nutrient in NUTRIENTS}

    #
    # Process data.
    #
    stage_keys = [key for key, _, _ in STAGES]
    for nutrient, tb in tables.items():
        # Keep only the country and year of the chart.
        tb = tb[(tb["country"].astype(str) == COUNTRY) & (tb["year"] == YEAR)].reset_index(drop=True)
        assert len(tb) == 1, f"Expected one row for {COUNTRY} in {YEAR} in {nutrient!r}, found {len(tb)}."
        assert set(stage_keys + NEGLIGIBLE_STAGES) <= set(tb.columns), f"Table {nutrient!r} lacks stages."
        for key in NEGLIGIBLE_STAGES:
            assert (tb[key].abs() <= MAX_NEGLIGIBLE_STAGE_SHARE * tb["food"]).all(), (
                f"Stage {key!r} is not negligible in {nutrient!r}, so it can't be left out."
            )
        tables[nutrient] = tb
    reference = tables["energy"]

    # The files' provenance, derived from the garden columns they are built from.
    feed_metadata = build_feed_metadata(
        title=f"Food supply chain ({METHOD} method)",
        columns={
            f"{name} ({tables[nutrient][key].metadata.short_unit})": tables[nutrient][key]
            for nutrient in NUTRIENTS
            for key, name, _ in STAGES
        },
        update_period_days=ds.metadata.update_period_days,
    )
    metadata = {
        "method": METHOD,
        # Every origin behind the chain (FAO's balances, OWID's population), not only the first.
        "sources": sorted({origin.attribution for origin in reference["food"].metadata.origins if origin.attribution}),
        "timeRange": {"start": YEAR, "end": YEAR},
        "units": {nutrient: tables[nutrient]["food"].metadata.unit for nutrient in NUTRIENTS},
        "stages": [{"key": key, "name": name, "direction": direction} for key, name, direction in STAGES],
        "dimensions": {"entities": [{"id": ENTITY_ID, "name": COUNTRY}]},
    }
    # The year, then for each unit one single-value array per stage.
    data = {
        nutrient: {key: [round(float(tables[nutrient][key].iloc[0]), NUM_DECIMALS[nutrient])] for key in stage_keys}
        for nutrient in NUTRIENTS
    }

    #
    # Save outputs.
    #
    save_json(add_feed_metadata(metadata, feed_metadata), f"{FILE_SLUG}.metadata.json")
    save_json({"years": [YEAR], **data}, f"{FILE_SLUG}.{ENTITY_ID}.json")
