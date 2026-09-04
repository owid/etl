"""Express the FAOSTAT Food Balance Sheets (FBS) as a chain of stages in kilocalories per person per day.

FBS gives, for each item, country and year, a balance in tonnes:

    production + imports - exports - stock variation
        = food + feed + seed + processing + other uses + losses + tourist consumption + residuals

but only "Food" is also given in kilocalories. This step converts every balance element to kilocalories with a
per-item energy factor, sums items into the stages of the chain (see STAGES), and divides by OWID population, so
that a waterfall chart can show where calories go between crop production and food available to eat.

Energy factors (kcal per 100 g) are reverse-engineered from FBS itself, as food supply in kcal divided by food
supply in tonnes, per item, country and year. The same factor is applied to all elements of an item, so the
balance identity above holds in kcal exactly as it holds in tonnes. Items whose direct food use is a sliver of
their production, and oils (whose implied factors exceed the physical maximum), use fixed factors from FAO's own
food composition tables instead (declared in `food_supply_chain.items.yml`). Implausible country-year factors
(outside a band around the item's pooled median) fall back to that median.

The curated item list, the role of each item (primary crop, derived vegetal product, animal product), and the
fixed factors live in `food_supply_chain.items.yml`. Changing the treatment of derived products (which items count
as "crop production") is a change to that file, not to this code.
"""

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

# Number of characters of item codes and element codes in the FAOSTAT garden tables.
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
    # Food supply per capita, in kcal per day and kg per year (both divided by the same population), used only to
    # reverse-engineer the energy factors.
    "0664pc": "food_kcal_per_capita_per_day",
    "0645pc": "food_kg_per_capita_per_year",
}
# Expected units of the elements above, as given in the garden table.
ELEMENT_UNITS = {
    "kilocalories per day per capita": ["0664pc"],
    "kilograms per year per capita": ["0645pc"],
    "tonnes": [code for code in ELEMENTS if not code.endswith("pc")],
}
# Balance elements that are converted to kcal and summed over items into a stage of the chain.
# NOTE: Stock variation is not read from FBS (it is only reported from 2010 onward) but derived from the identity
# production + imports - exports - domestic supply, which holds for all years; the reported value is used to check it.
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
# Elements that are summed over all items into a stage of the same name.
SHARED_STAGES = [element for element in BALANCE_ELEMENTS if element != "production"]
# Production is split by item role into three stages.
PRODUCTION_STAGES = {
    "primary_crop": "crop_production",
    "derived_vegetal": "derived_vegetal_production",
    "animal_product": "animal_products",
}
# Output columns, in chain order.
STAGES = [
    "crop_production",
    "imports",
    "exports",
    "stock_variation",
    "seed",
    "losses",
    "other_uses",
    "processing",
    "feed",
    "animal_products",
    "tourist_consumption",
    "residuals",
    "food",
    "derived_vegetal_production",
    "balancing_difference",
]
# Stages that are subtracted along the chain (all others are added).
SUBTRACTED_STAGES = [
    "exports",
    "stock_variation",
    "seed",
    "losses",
    "other_uses",
    "processing",
    "feed",
    "tourist_consumption",
    "residuals",
]

# A reverse-engineered factor is accepted if it lies within this multiplicative band around the item's pooled
# median (over all countries and years); otherwise the median is used. Guards against country-years where a tiny
# food quantity is rounded to zero (or near zero) in FBS, which gives absurd factors.
FACTOR_BAND = 2.0
# No food has more energy than pure fat: 902 kcal per 100 g is the highest factor in FAO's food composition tables
# (rendered animal fats and fish oils). Reverse-engineered factors above it are artifacts and fall back to the median.
FACTOR_MAX = 902
# Item codes of FAO's aggregate items used to check that the curated items partition the total food supply.
TOTAL_ITEM_CODE = "00002901"
VEGETAL_ITEM_CODE = "00002903"
ANIMAL_ITEM_CODE = "00002941"
# Maximum relative deviation between the sum of curated items and FAO's own aggregate, for World.
PARTITION_TOLERANCE = 0.01
# Tolerance for the identity checks in tonnes (relative to the item's domestic supply, plus FAO's rounding to 1,000 t).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# Conversion from tonnes to units of 100 g (the unit of the energy factors).
HUNDRED_GRAMS_PER_TONNE = 10_000
DAYS_PER_YEAR = 365


def _pad_code(code: int) -> str:
    return str(code).zfill(N_CHARACTERS_ITEM_CODE)


def load_items_config() -> tuple[Table, dict[str, str], dict[str, str]]:
    """Load the curated items file.

    Returns a table of curated items (one row per item, indexed by padded item code), the codes of excluded
    items (code -> name) and the codes of aggregate group items (code -> name).
    """
    with open(paths.side_file("food_supply_chain.items.yml")) as f:
        config = yaml.safe_load(f)
    assert set(config) == {"items", "excluded", "aggregate_groups"}, "Unexpected top-level keys in items file."

    allowed_keys = {"code", "name", "role", "fao_group", "fixed_factor_kcal_per_100g"}
    for item in config["items"]:
        assert {"code", "name", "role"} <= set(item) <= allowed_keys, f"Unexpected keys in item: {item}"
        assert item["role"] in PRODUCTION_STAGES, f"Unknown role in item: {item}"
        assert item.get("fao_group", "vegetal") in {"vegetal", "animal"}, f"Unknown fao_group in item: {item}"
    items = pd.DataFrame(config["items"])
    items["item_code"] = items["code"].map(_pad_code)
    # FAO group (for the partition check) defaults to the natural group of the role.
    natural_group = items["role"].map(
        {"primary_crop": "vegetal", "derived_vegetal": "vegetal", "animal_product": "animal"}
    )
    if "fao_group" not in items.columns:
        items["fao_group"] = None
    items["fao_group"] = items["fao_group"].fillna(natural_group)
    items = items.set_index("item_code", verify_integrity=True)

    excluded = {_pad_code(item["code"]): item["name"] for item in config["excluded"]}
    groups = {_pad_code(item["code"]): item["name"] for item in config["aggregate_groups"]}
    all_codes = list(items.index) + list(excluded) + list(groups)
    assert len(all_codes) == len(set(all_codes)), "An item code appears in more than one list of the items file."

    return Table(items), excluded, groups


def sanity_check_inputs(tb: Table, items: Table, excluded: dict[str, str], groups: dict[str, str]) -> None:
    """Check the FBS table against our expectations and against the curated items file."""
    # Elements and units.
    elements = tb[["element_code", "unit"]].drop_duplicates().set_index("element_code")["unit"]
    for unit, codes in ELEMENT_UNITS.items():
        for code in codes:
            assert code in elements.index, f"Element {code} ({ELEMENTS[code]}) not found in FBS table."
            assert elements[code] == unit, (
                f"Element {code} ({ELEMENTS[code]}) has unit {elements[code]!r}, expected {unit!r}."
            )

    # Every item code in the table is either curated, excluded or an aggregate group, and carries the expected name.
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
    """Reshape the FBS table to one row per (country, year, item) with one column per element, for curated items."""
    tb = tb[tb["item_code"].isin(items.index) & tb["element_code"].isin(ELEMENTS)].reset_index(drop=True)
    tb = tb[["country", "year", "item_code", "element_code", "value"]].astype({"country": str, "item_code": str})
    tb = tb.pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in ELEMENTS.items()})
    assert set(ELEMENTS.values()) <= set(tb.columns), "Some elements are missing after pivoting."

    # A missing balance element means the element is not part of that item's balance (e.g. no seed for meat); treat
    # it as zero so that the identity can be evaluated. Per-capita food (used only for factors) keeps its nans.
    tonnes_columns = [name for code, name in ELEMENTS.items() if not code.endswith("pc")]
    for column in tonnes_columns:
        tb[column] = tb[column].fillna(0)

    # Stock variation from the identity (see BALANCE_ELEMENTS). FAO's sign convention: positive means stocks grew.
    tb["stock_variation"] = tb["production"] + tb["imports"] - tb["exports"] - tb["domestic_supply"]

    # Attach the item's role and fixed factor.
    tb = tb.merge(items[["role", "fixed_factor_kcal_per_100g"]].reset_index(), on="item_code", how="left")
    assert tb["role"].notnull().all(), "Some rows have no role (item missing from items file)."

    return tb


def sanity_check_balance_identity(tb: Table) -> None:
    """Check that FBS balances close in tonnes, and that the derived stock variation matches the reported one."""
    uses = tb[["food", "feed", "seed", "processing", "other_uses", "losses", "tourist_consumption", "residuals"]].sum(
        axis=1
    )
    tolerance = IDENTITY_RELATIVE_TOLERANCE * tb["domestic_supply"].abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    gap = (tb["domestic_supply"] - uses).abs()
    share_open = (gap > tolerance).mean()
    error = f"Domestic supply differs from the sum of uses in {100 * share_open:.1f}% of item balances."
    assert share_open < 0.02, error

    # Where FBS reports stock variation (2010 onward), it must match the derived one. The few mismatches are in
    # OWID region aggregates, whose elements are summed over slightly different sets of member countries.
    reported = tb[tb["stock_variation_reported"] != 0]
    mismatch = (reported["stock_variation"] - reported["stock_variation_reported"]).abs() > tolerance[reported.index]
    error = f"Derived stock variation differs from the reported one in {100 * mismatch.mean():.2f}% of item balances."
    assert mismatch.mean() < 0.005, error


def add_energy_factors(tb: Table) -> Table:
    """Add a column of energy factors, in kcal per 100 g, for each (country, year, item)."""
    # Reverse-engineered factor: food kcal per person per day, over food kg per person per year.
    tb["factor_raw"] = (tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR) / (tb["food_kg_per_capita_per_year"] * 10)
    tb.loc[~np.isfinite(tb["factor_raw"]) | (tb["factor_raw"] <= 0), "factor_raw"] = np.nan

    # Pooled median per item, over all countries and years with a finite factor.
    tb["factor_median"] = tb.groupby("item_code", observed=True)["factor_raw"].transform("median")
    within_band = (
        (tb["factor_raw"] >= tb["factor_median"] / FACTOR_BAND)
        & (tb["factor_raw"] <= tb["factor_median"] * FACTOR_BAND)
        & (tb["factor_raw"] <= FACTOR_MAX)
    )

    tb["factor_source"] = "fallback_median"
    tb["factor"] = tb["factor_median"]
    tb.loc[within_band, "factor_source"] = "reverse_engineered"
    tb.loc[within_band, "factor"] = tb.loc[within_band, "factor_raw"]
    fixed = tb["fixed_factor_kcal_per_100g"].notnull()
    tb.loc[fixed, "factor_source"] = "fixed"
    tb.loc[fixed, "factor"] = tb.loc[fixed, "fixed_factor_kcal_per_100g"]

    error = "Some items have no energy factor at all (no food use anywhere and no fixed factor)."
    assert tb["factor"].notnull().all(), error + f" {sorted(set(tb[tb['factor'].isnull()]['item_code']))}"

    return tb


def sanity_check_energy_factors(tb: Table, items: Table) -> None:
    """Check the factor distribution and log a per-item table of factors for review."""
    error = f"Energy factors must be positive and cannot exceed {FACTOR_MAX} kcal per 100 g."
    assert (tb["factor"] > 0).all() and (tb["factor"] <= FACTOR_MAX).all(), error

    # Share of production (tonnes) converted with a fallback (median) factor, per item and overall.
    fallback_production = tb["production"].where(tb["factor_source"] == "fallback_median", 0)
    share_fallback = fallback_production.sum() / tb["production"].sum()
    error = f"{100 * share_fallback:.1f}% of production (in tonnes) was converted with fallback factors."
    assert share_fallback < 0.02, error

    summary = tb.groupby("item_code", observed=True).agg(
        median=("factor_median", "first"),
        p10=("factor_raw", lambda x: x.quantile(0.1)),
        p90=("factor_raw", lambda x: x.quantile(0.9)),
        fixed=("fixed_factor_kcal_per_100g", "first"),
        production=("production", "sum"),
        production_fallback=("production", lambda x: x[tb.loc[x.index, "factor_source"] == "fallback_median"].sum()),
    )
    summary["share_fallback_pct"] = 100 * summary["production_fallback"] / summary["production"]
    summary = summary.join(items[["name", "role"]]).set_index("name")[
        ["role", "median", "p10", "p90", "fixed", "share_fallback_pct"]
    ]
    with pd.option_context("display.max_rows", None, "display.width", 200):
        log.info("food_supply_chain.energy_factors_kcal_per_100g\n" + summary.round(1).to_string())


def build_chain(tb: Table) -> Table:
    """Convert balance elements to kcal, sum items into stages, and express them per person per day."""
    kcal = tb[["country", "year"]].copy()
    for element in BALANCE_ELEMENTS:
        kcal[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["factor"]
    # Split production by role.
    for role, stage in PRODUCTION_STAGES.items():
        kcal[stage] = kcal["production"].where(tb["role"] == role, 0)
    kcal = kcal.drop(columns=["production"])

    chain = kcal.groupby(["country", "year"], observed=True, as_index=False).sum(min_count=1)

    # The chain from crop production to food, with derived production left out (it re-appears from processing).
    chain_end = chain["crop_production"] + chain["animal_products"]
    for stage in SHARED_STAGES:
        if stage == "food":
            continue
        chain_end = chain_end - chain[stage] if stage in SUBTRACTED_STAGES else chain_end + chain[stage]
    chain["balancing_difference"] = chain["food"] - chain_end

    # Per person per day, using OWID population for all stages.
    chain = paths.regions.add_population(chain, warn_on_missing_countries=False)
    missing_population = sorted(set(chain[chain["population"].isnull()]["country"]))
    if missing_population:
        log.warning(f"Dropping entities without OWID population: {missing_population}")
        chain = chain.dropna(subset=["population"]).reset_index(drop=True)
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    chain = chain[["country", "year"] + STAGES]

    return chain


def sanity_check_partition(tb_fbsc: Table, items: Table) -> None:
    """Check that food supply (kcal) summed over curated items reproduces FAO's own aggregate items, for World."""
    world = tb_fbsc[(tb_fbsc["country"] == "World") & (tb_fbsc["element_code"] == "0664pc")]
    world = world[["year", "item_code", "value"]].astype({"item_code": str})
    fao = world.pivot(index="year", columns="item_code", values="value")
    curated = world[world["item_code"].isin(items.index)].merge(items[["fao_group"]].reset_index(), on="item_code")
    ours = curated.pivot_table(index="year", columns="fao_group", values="value", aggfunc="sum")
    ours["total"] = ours["vegetal"] + ours["animal"]
    checks = {"total": TOTAL_ITEM_CODE, "vegetal": VEGETAL_ITEM_CODE, "animal": ANIMAL_ITEM_CODE}
    for group, code in checks.items():
        deviation = ((ours[group] - fao[code]) / fao[code]).abs()
        error = f"Curated items do not reproduce FAO's {group} food supply for World; max deviation {100 * deviation.max():.2f}%."
        assert deviation.max() < PARTITION_TOLERANCE, error


def sanity_check_outputs(tb: Table, tb_fbsc: Table) -> None:
    error = "Output has fully-nan columns."
    assert tb.columns[tb.isna().all()].empty, error
    error = "Duplicate (country, year) rows."
    assert not tb.duplicated(subset=["country", "year"]).any(), error
    # Stages that cannot be negative (stock variation and residuals can).
    for stage in [s for s in STAGES if s not in ["stock_variation", "residuals", "balancing_difference"]]:
        error = f"Negative values in stage {stage!r}: {tb[tb[stage] < 0][['country', 'year', stage]].head()}"
        assert (tb[stage].fillna(0) >= 0).all(), error
    # The chain closes up to the derived production left out of it (plus FAO's rounding).
    world = tb[tb["country"] == "World"].set_index("year")
    gap = (world["balancing_difference"] - world["derived_vegetal_production"]).abs() / world["crop_production"]
    error = (
        f"Chain does not close for World beyond derived production; max gap {100 * gap.max():.2f}% of crop production."
    )
    assert gap.max() < 0.01, error
    # Our food stage should reproduce FAO's own food supply (its "Grand Total" item). The two differ only through
    # the fixed factors (oils in particular, where FAO's implied factors are above the physical maximum) and through
    # the population series used. For World this is under 1%.
    fao_total = tb_fbsc[
        (tb_fbsc["country"] == "World")
        & (tb_fbsc["element_code"] == "0664pc")
        & (tb_fbsc["item_code"] == TOTAL_ITEM_CODE)
    ].set_index("year")["value"]
    deviation = (world["food"] - fao_total) / fao_total
    log.info(
        "food_supply_chain.food_vs_fao_total",
        max_deviation_pct=round(100 * deviation.abs().max(), 2),
        latest_year=int(deviation.index.max()),
        latest_deviation_pct=round(100 * deviation[deviation.index.max()], 2),
    )
    error = f"World food supply deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    assert deviation.abs().max() < 0.02, error


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
    tb = add_energy_factors(tb)
    sanity_check_energy_factors(tb, items=items)
    tb = build_chain(tb)
    sanity_check_outputs(tb, tb_fbsc=tb_fbsc)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
