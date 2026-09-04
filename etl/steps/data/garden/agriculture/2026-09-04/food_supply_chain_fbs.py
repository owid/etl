"""FAOSTAT Food Balance Sheets (FBS) as a chain of stages from crop production to food, per person per day.

This step reproduces, with ETL data and code, the method of the FBS-based waterfall prototype: FBS elements in
tonnes are converted with per-item densities reverse-engineered from FBS itself, and summed into the stages of a
chain from crop production to food available to eat. The chain is built three times, in three units, one table each:
    energy   kilocalories per person per day
    protein  grams of protein per person per day
    mass     kilograms per person per day (the balance in tonnes, with no conversion at all)

ASSUMPTIONS AND NUMBERS THAT GO INTO THE CALCULATION
-----------------------------------------------------
1. The balance identity. For every item, country and year, FBS reports (in tonnes)
       production + imports - exports - stock variation
         = food + feed + seed + processing + other uses + losses + tourist consumption + residuals.
   Stock variation is derived as production + imports - exports - domestic supply, because FBS only reports it from
   2010 onward; where reported, the derived value is checked against it.

2. Density of an item (kcal, or grams of protein, per 100 g) = food supply of the nutrient per person per day x 365
   / (food supply in kg per person per year x 10), per item, country and year. The same density is applied to every
   element of the item, so the identity above holds in the nutrient exactly as in tonnes and the chain closes by
   construction. For mass there is no density: tonnes are converted to kilograms.

3. The only rejection rule for a density is a physical ceiling: nothing edible has more energy than pure fat
   (920 kcal per 100 g) or more than 100 g of protein per 100 g. A density above the ceiling is a broken FAOSTAT cell
   (FAO's nutrient and tonnage figures for the same item disagree). Such cells fall back to the country's median
   density for the item over all years, then to the item's median over all countries and years. Nothing merely
   unusual is second-guessed.
   Consequence: where FAO's own figures are inconsistent (vegetable oils in the United States above all), the food
   stage lands below FAO's published food supply.

4. Items. Every FBS item code must appear in `food_supply_chain_fbs.items.yml`, as a chain item with a role, as an
   excluded item, or as an aggregate group (which must not be added, to avoid double counting). Three items get
   no nutrient at all, because nobody eats them and FAO gives them no food energy: "Palm kernels" (which carries the
   whole oil palm harvest; the oil palm enters the chain as palm oil and palm kernel oil, tagged as crops),
   "Alcohol, Non-Food" and "Meat, Aquatic Mammals". In the mass table, excluded items are left out too.

5. Production is split by the item's role, so that nothing is counted twice:
     crop      -> "crop_production", the start of the chain;
     animal    -> "animal_products", added after feed has been subtracted;
     processed -> netted against the processing outflow: "processing_net" = processing - production of processed
                  items (oils, sugar, alcoholic beverages, butter, cream, and others), so the bar shows only what goes
                  into processing and does not come back as a product.

6. FAOSTAT rounds tonnages to 1,000 t, so its supply and uses sides do not close exactly. That gap is folded into
   "residuals" (FAO's own balancing item) so that the chain lands exactly on "food". Its size is kept in
   "balancing_difference" for quality control.

7. All stages are divided by the same population, from OWID's population dataset, and by 365 days. FAO's own
   regional aggregates are dropped; OWID countries and regions are kept.

Known limitations, inherited from FBS: oilseed cakes and other feed by-products are not FBS items, so what goes
oilseed -> cake -> feed appears under processing, not feed; and FBS "Losses" stop at the retail shelf.
"""

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

# Number of characters of item codes in the FAOSTAT garden tables.
N_CHARACTERS_ITEM_CODE = 8

# FBS elements used by this step (garden element codes) and their short names in this step.
ELEMENTS = {
    "005511": "production",
    "005611": "imports",
    "005911": "exports",
    "005301": "domestic_supply",
    "005072": "stock_variation_reported",
    "005527": "seed",
    "005123": "losses",
    "005154": "other_uses",
    "005131": "processing",
    "005521": "feed",
    "005171": "tourist_consumption",
    "005170": "residuals",
    "005142": "food",
    # Food supply per capita (all divided by the same population), used only to reverse-engineer densities.
    "0664pc": "food_kcal_per_capita_per_day",
    "0674pc": "food_protein_g_per_capita_per_day",
    "0645pc": "food_kg_per_capita_per_year",
}
PER_CAPITA_ELEMENTS = ["0664pc", "0674pc", "0645pc"]
# Expected units of the elements above, as given in the garden table.
ELEMENT_UNITS = {
    "kilocalories per day per capita": ["0664pc"],
    "grams of protein per day per capita": ["0674pc"],
    "kilograms per year per capita": ["0645pc"],
    "tonnes": [code for code in ELEMENTS if code not in PER_CAPITA_ELEMENTS],
}
# The three units the chain is built in. `numerator` is the per-capita food supply column the density is derived
# from (None for mass); `ceiling` is the physical maximum density; `per_100g_to_per_capita` explains itself.
NUTRIENTS = {
    "energy": {"numerator": "food_kcal_per_capita_per_day", "ceiling": 920, "unit": "kilocalories per person per day"},
    "protein": {
        "numerator": "food_protein_g_per_capita_per_day",
        "ceiling": 100,
        "unit": "grams of protein per person per day",
    },
    "mass": {"numerator": None, "ceiling": None, "unit": "kilograms per person per day"},
}
# Balance elements that are converted and summed over items.
BALANCE_ELEMENTS = [
    "production",
    "imports",
    "exports",
    "stock_variation",
    "seed",
    "losses",
    "other_uses",
    "processing",
    "feed",
    "tourist_consumption",
    "residuals",
    "food",
]
USES = ["food", "feed", "seed", "processing", "other_uses", "losses", "tourist_consumption", "residuals"]
# Item roles (see assumption 5) and the stage their production goes to.
ROLES = {"crop": "crop_production", "animal": "animal_products", "processed": "processed_production"}
# Output columns, in chain order, as magnitudes in FAO's sign convention; SUBTRACTED_STAGES are subtracted along the chain.
STAGES = [
    "crop_production",
    "imports",
    "exports",
    "stock_variation",
    "seed",
    "losses",
    "other_uses",
    "processing_net",
    "feed",
    "animal_products",
    "tourist_consumption",
    "residuals",
    "food",
    "balancing_difference",
]
SUBTRACTED_STAGES = [
    "exports",
    "stock_variation",
    "seed",
    "losses",
    "other_uses",
    "processing_net",
    "feed",
    "tourist_consumption",
    "residuals",
]
# FAO's aggregate items used to check that the curated items partition the total food supply.
TOTAL_ITEM_CODE = "00002901"
VEGETAL_ITEM_CODE = "00002903"
ANIMAL_ITEM_CODE = "00002941"
PARTITION_TOLERANCE = 0.01
# Tolerance for the identity checks in tonnes (relative to the item's domestic supply, plus FAO's rounding).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
HUNDRED_GRAMS_PER_TONNE = 10_000
KG_PER_TONNE = 1000
DAYS_PER_YEAR = 365


def _pad_code(code: int) -> str:
    return str(code).zfill(N_CHARACTERS_ITEM_CODE)


def load_items_config() -> tuple[Table, dict[str, str], dict[str, str]]:
    """Load the curated items file: chain items (indexed by padded code), excluded items and aggregate groups."""
    with open(paths.side_file("food_supply_chain_fbs.items.yml")) as f:
        config = yaml.safe_load(f)
    assert set(config) == {"items", "excluded", "aggregate_groups"}, "Unexpected top-level keys in items file."

    for item in config["items"]:
        assert {"code", "name", "role"} <= set(item) <= {"code", "name", "role", "fao_group"}, (
            f"Unexpected keys: {item}"
        )
        assert item["role"] in ROLES, f"Unknown role in item: {item}"
        assert item.get("fao_group", "vegetal") in {"vegetal", "animal"}, f"Unknown fao_group in item: {item}"
    items = pd.DataFrame(config["items"])
    items["item_code"] = items["code"].map(_pad_code)
    natural_group = items["role"].map({"crop": "vegetal", "processed": "vegetal", "animal": "animal"})
    if "fao_group" not in items.columns:
        items["fao_group"] = np.nan
    items["fao_group"] = items["fao_group"].fillna(natural_group)
    items = items.set_index("item_code", verify_integrity=True)

    excluded = {_pad_code(item["code"]): item["name"] for item in config["excluded"]}
    groups = {_pad_code(item["code"]): item["name"] for item in config["aggregate_groups"]}
    all_codes = list(items.index) + list(excluded) + list(groups)
    assert len(all_codes) == len(set(all_codes)), "An item code appears in more than one list of the items file."

    return Table(items), excluded, groups


def sanity_check_inputs(tb: Table, items: Table, excluded: dict[str, str], groups: dict[str, str]) -> None:
    elements = tb[["element_code", "unit"]].drop_duplicates().set_index("element_code")["unit"]
    for unit, codes in ELEMENT_UNITS.items():
        for code in codes:
            assert code in elements.index, f"Element {code} ({ELEMENTS[code]}) not found in FBS table."
            assert elements[code] == unit, f"Element {code} has unit {elements[code]!r}, expected {unit!r}."

    table_items = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"].astype(str)
    expected_names = {**items["name"].to_dict(), **excluded, **groups}
    unknown = sorted(set(table_items.index) - set(expected_names))
    assert not unknown, f"FBS items not covered by the items file: {[(c, table_items[c]) for c in unknown]}"
    missing = sorted(set(expected_names) - set(table_items.index))
    assert not missing, (
        f"Items in the items file that are not in the FBS table: {[(c, expected_names[c]) for c in missing]}"
    )
    renamed = {code: (name, table_items[code]) for code, name in expected_names.items() if table_items[code] != name}
    assert not renamed, f"FAO item name no longer matches the curated name (code: (expected, found)): {renamed}"


def prepare_balance_table(tb: Table, items: Table) -> Table:
    """Reshape the FBS table to one row per (country, year, item) with one column per element, for chain items."""
    tb = tb[tb["item_code"].isin(items.index) & tb["element_code"].isin(ELEMENTS)].reset_index(drop=True)
    tb = tb[["country", "year", "item_code", "element_code", "value"]].astype(
        {"country": str, "item_code": str, "value": float}
    )
    tb = tb.pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in ELEMENTS.items()})
    assert set(ELEMENTS.values()) <= set(tb.columns), "Some elements are missing after pivoting."

    # A missing balance element means the element is not part of that item's balance (e.g. no seed for meat); treat
    # it as zero so that the identity can be evaluated. Per-capita food (used only for densities) keeps its nans.
    tonnes_columns = [name for code, name in ELEMENTS.items() if code not in PER_CAPITA_ELEMENTS]
    tb[tonnes_columns] = tb[tonnes_columns].fillna(0)

    # Stock variation from the identity (assumption 1). FAO's sign convention: positive means stocks grew.
    tb["stock_variation"] = tb["production"] + tb["imports"] - tb["exports"] - tb["domestic_supply"]

    tb = tb.merge(items[["role"]].reset_index(), on="item_code", how="left")
    assert tb["role"].notnull().all(), "Some rows have no role (item missing from items file)."
    return tb


def sanity_check_balance_identity(tb: Table) -> None:
    """Check that FBS balances close in tonnes, and that the derived stock variation matches the reported one."""
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * tb["domestic_supply"].abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_open = ((tb["domestic_supply"] - uses).abs() > tolerance).mean()
    assert share_open < 0.02, (
        f"Domestic supply differs from the sum of uses in {100 * share_open:.1f}% of item balances."
    )

    # Where FBS reports stock variation (2010 onward), it must match the derived one. The few mismatches are in
    # OWID region aggregates, whose elements are summed over slightly different sets of member countries.
    reported = tb[tb["stock_variation_reported"] != 0]
    mismatch = (reported["stock_variation"] - reported["stock_variation_reported"]).abs() > tolerance[reported.index]
    assert mismatch.mean() < 0.005, (
        f"Derived stock variation differs from the reported one in {100 * mismatch.mean():.2f}% of item balances."
    )


def add_densities(tb: Table, nutrient: str) -> Table:
    """Add `density` (nutrient per 100 g) and `density_source` for each (country, year, item). Assumptions 2 and 3."""
    tb = tb.copy()
    config = NUTRIENTS[nutrient]
    if config["numerator"] is None:
        # Mass: kilograms per 100 g, so that tonnes x HUNDRED_GRAMS_PER_TONNE x density gives kilograms.
        tb["density"] = KG_PER_TONNE / HUNDRED_GRAMS_PER_TONNE
        tb["density_source"] = "mass"
        return tb

    raw = (tb[config["numerator"]] * DAYS_PER_YEAR) / (tb["food_kg_per_capita_per_year"] * 10)
    raw = raw.where(np.isfinite(raw) & (raw >= 0))
    within_ceiling = raw.where(raw <= config["ceiling"])
    # A density of exactly zero is real for oils and sugars (no protein), but it is also what a tiny food quantity
    # rounded to zero produces, so zeros are not used directly: they enter the medians, which come out as zero
    # for items that truly have none of the nutrient and as the usual value otherwise.
    accepted = within_ceiling.where(within_ceiling > 0)
    country_median = within_ceiling.groupby([tb["country"], tb["item_code"]], observed=True).transform("median")
    item_median = within_ceiling.groupby(tb["item_code"], observed=True).transform("median")
    tb["density_raw"] = raw
    tb["density"] = accepted.fillna(country_median).fillna(item_median)
    tb["density_source"] = np.select(
        [accepted.notnull(), country_median.notnull(), item_median.notnull()],
        ["direct", "country_median", "item_median"],
        default="none",
    )
    missing = tb[tb["density"].isnull()]
    assert missing.empty, f"Items with no {nutrient} density at all: {sorted(set(missing['item_code']))}"
    return tb


def sanity_check_densities(tb: Table, items: Table, nutrient: str) -> None:
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."

    provenance = tb["domestic_supply"].abs().groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    log.info(f"food_supply_chain_fbs.{nutrient}_density_provenance_pct_of_domestic_supply", **provenance.to_dict())
    assert provenance.get("direct", 0) > 90, (
        f"Only {provenance.get('direct', 0):.1f}% of domestic supply uses a direct {nutrient} density."
    )

    summary = tb.groupby("item_code", observed=True).agg(
        item_median=("density", "median"),
        p10=("density_raw", lambda x: x.quantile(0.1)),
        p90=("density_raw", lambda x: x.quantile(0.9)),
        share_rejected_pct=("density_source", lambda x: 100 * (x != "direct").mean()),
    )
    summary = summary.join(items[["name", "role"]]).set_index("name")[
        ["role", "item_median", "p10", "p90", "share_rejected_pct"]
    ]
    with pd.option_context("display.max_rows", None, "display.width", 200):
        log.info(f"food_supply_chain_fbs.{nutrient}_densities_per_100g\n" + summary.round(1).to_string())


def build_chain(tb: Table, nutrient: str) -> Table:
    """Convert balance elements with the densities, sum items into stages, and express them per person per day."""
    converted = tb[["country", "year"]].copy()
    for element in BALANCE_ELEMENTS:
        converted[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["density"]
    for role, stage in ROLES.items():
        converted[stage] = converted["production"].where(tb["role"] == role, 0)
    converted = converted.drop(columns=["production"])

    chain = converted.groupby(["country", "year"], observed=True, as_index=False).sum(min_count=1)

    # Assumption 5: processing net of the production of processed items.
    chain["processing_net"] = chain["processing"] - chain["processed_production"]
    chain = chain.drop(columns=["processing", "processed_production"])

    # Assumption 6: fold FAO's rounding gap into residuals so that the chain lands exactly on food.
    chain_end = chain["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - chain[stage] if stage in SUBTRACTED_STAGES else chain_end + chain[stage]
    chain["balancing_difference"] = chain["food"] - chain_end
    chain["residuals"] = chain["residuals"] - chain["balancing_difference"]

    # Per person per day, using OWID population for all stages (assumption 7).
    chain = paths.regions.add_population(chain, warn_on_missing_countries=False)
    missing_population = sorted(set(chain[chain["population"].isnull()]["country"]))
    if missing_population:
        log.warning(f"Dropping entities without OWID population: {missing_population}")
        chain = chain.dropna(subset=["population"]).reset_index(drop=True)
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    return chain[["country", "year"] + STAGES]


def sanity_check_partition(tb_fbsc: Table, items: Table) -> None:
    """Check that food supply (kcal) summed over chain items reproduces FAO's own aggregate items, for World."""
    world = tb_fbsc[(tb_fbsc["country"] == "World") & (tb_fbsc["element_code"] == "0664pc")]
    world = world[["year", "item_code", "value"]].astype({"item_code": str, "value": float})
    fao = world.pivot(index="year", columns="item_code", values="value")
    curated = world[world["item_code"].isin(items.index)].merge(items[["fao_group"]].reset_index(), on="item_code")
    ours = curated.pivot_table(index="year", columns="fao_group", values="value", aggfunc="sum")
    ours["total"] = ours["vegetal"] + ours["animal"]
    for group, code in {"total": TOTAL_ITEM_CODE, "vegetal": VEGETAL_ITEM_CODE, "animal": ANIMAL_ITEM_CODE}.items():
        deviation = ((ours[group] - fao[code]) / fao[code]).abs()
        assert deviation.max() < PARTITION_TOLERANCE, (
            f"Chain items do not reproduce FAO's {group} food supply for World; max deviation {100 * deviation.max():.2f}%."
        )


def sanity_check_outputs(tb: Table, tb_fbsc: Table, nutrient: str) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has fully-nan columns."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    for stage in [
        s for s in STAGES if s not in ["stock_variation", "residuals", "processing_net", "balancing_difference"]
    ]:
        # FAO occasionally reports a negative flow (Iraq 2010 wheat exports, for one); small negatives are tolerated.
        assert (tb[stage].fillna(0) >= -0.01 * tb["food"].abs()).all(), (
            f"Negative values in stage {stage!r} ({nutrient})."
        )

    # The chain lands exactly on food once the rounding gap is folded into residuals.
    chain_end = tb["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - tb[stage] if stage in SUBTRACTED_STAGES else chain_end + tb[stage]
    assert (chain_end - tb["food"]).abs().max() < 1e-3 * tb["food"].abs().max(), "Chain does not land on food."
    world = tb[tb["country"] == "World"].set_index("year")
    gap = (world["balancing_difference"] / world["food"]).abs()
    assert gap.max() < 0.02, f"FAO rounding gap for World is up to {100 * gap.max():.2f}% of food ({nutrient})."

    # Our food stage against FAO's own food supply (its "Grand Total" item), for energy and protein.
    fao_element = {"energy": "0664pc", "protein": "0674pc"}.get(nutrient)
    if fao_element is None:
        return
    fao_total = (
        tb_fbsc[
            (tb_fbsc["country"] == "World")
            & (tb_fbsc["element_code"] == fao_element)
            & (tb_fbsc["item_code"] == TOTAL_ITEM_CODE)
        ]
        .set_index("year")["value"]
        .astype(float)
    )
    deviation = (world["food"] - fao_total) / fao_total
    log.info(
        f"food_supply_chain_fbs.{nutrient}_food_vs_fao_total",
        max_deviation_pct=round(100 * deviation.abs().max(), 2),
        latest_year=int(deviation.index.max()),
        latest_deviation_pct=round(100 * deviation[deviation.index.max()], 2),
    )
    assert deviation.abs().max() < 0.03, (
        f"World food {nutrient} deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    )


def run() -> None:
    #
    # Load inputs.
    #
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc", safe_types=False)
    items, excluded, groups = load_items_config()

    #
    # Process data.
    #
    # Keep OWID countries and regions; drop FAO's own regional aggregates, which duplicate them.
    tb_fbsc = tb_fbsc[~tb_fbsc["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    sanity_check_inputs(tb_fbsc, items=items, excluded=excluded, groups=groups)
    sanity_check_partition(tb_fbsc, items=items)

    tb = prepare_balance_table(tb_fbsc, items=items)
    sanity_check_balance_identity(tb)

    tables = []
    for nutrient in NUTRIENTS:
        tb_nutrient = add_densities(tb, nutrient=nutrient)
        if nutrient != "mass":
            sanity_check_densities(tb_nutrient, items=items, nutrient=nutrient)
        chain = build_chain(tb_nutrient, nutrient=nutrient)
        sanity_check_outputs(chain, tb_fbsc=tb_fbsc, nutrient=nutrient)
        tables.append(chain.format(["country", "year"], short_name=nutrient))

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
