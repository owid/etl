"""FAO Supply Utilization Accounts (SCL) as a chain of stages from crop production to food, per person per day.

Same chain as `food_supply_chain_fbs` (crop production, trade, stock changes, seed, losses, other uses, processing,
feed, animal products, food), built from the Supply Utilization Accounts instead of the Food Balance Sheets. SCL
reports individual commodities rather than FBS's item groups, and it includes the by-products that FBS leaves out:
oilseed cakes, brans, gluten feed. With those items present, what goes from oilseeds to cakes to animals appears
under feed, where it belongs, instead of vanishing inside processing. The chain is built in three units, one table
each: energy (kcal per person per day), protein (grams per person per day) and mass (kilograms per person per day,
the balance in tonnes with no conversion at all).

ASSUMPTIONS AND NUMBERS THAT GO INTO THE CALCULATION
-----------------------------------------------------
1. The balance identity. For every item, country and year, SCL reports (in tonnes)
       production + imports - exports - stock variation
         = food + feed + seed + processing + other uses + losses + tourist consumption + residuals.
   This is checked item by item, not assumed.

2. Roles. FAO's own item groups tag every SCL item as "Crops, primary", "Livestock primary", "Crops processed" or
   "Livestock processed". Production enters the chain by role:
     crop      -> "crop_production", the start of the chain;
     animal    -> "animal_products", added after feed has been subtracted;
     processed -> netted against processing: "processing_net" = processing - production of processed items, so
                  that the bar shows only what goes into a factory and does not come out as a product.
   One override, listed in the items file: cotton seed is treated as a crop, because the seed cotton it is ginned
   from is a fibre crop and not in SCL.

3. Density of an item (kcal, or grams of protein, per 100 g) = food supply of the nutrient (FAO's "Calories/Year"
   and "Proteins/Year", in kilocalories and tonnes) / food supply in tonnes, per item, country and year. The same density is applied to every
   flow of the item, so the identity holds in the nutrient exactly as in tonnes and the chain closes by
   construction. The only rejection rule is a physical ceiling (920 kcal, or 100 g of protein, per 100 g); rejected
   cells fall back to the country's median for the item over all years, then to the item's median over all
   countries and years. For mass there is no density: tonnes are converted to kilograms.

4. Items nobody eats (cakes, brans, ethanol, refining residues) have no food figures to reverse-engineer from. They
   get the energy and protein of the human food they would be if eaten, from USDA's food composition tables:
   numbers and sources are in `food_supply_chain_scl.items.yml`. Items that are never food (castor, tung, kapok,
   jojoba, wool grease) get zero energy and protein, which removes them from every flow consistently. In the mass
   table every item counts as it is.

5. Crops that are not eaten as harvested (paddy rice, sugar cane and beet, oil palm fruit, rapeseed, cotton seed)
   have a food-based density that rests on a sliver of the crop, or none at all, and it is far below the products
   they yield. Their density is derived from those products instead: the nutrient in the family's products, minus
   that of intermediate products processed further within the family, over the tonnes of crop that went into
   processing, for World each year, applied to every country. The crop-to-product links are in the items file.

6. Fish and seafood are not in SCL. The FBS fish items are spliced in, with their FBS densities under the same rules,
   for the entities and years that SCL covers.

7. FAOSTAT rounds tonnages, so balances do not close exactly. The gap is folded into "residuals" so the chain lands
   exactly on "food"; its size is kept in "balancing_difference".

8. Regions. OWID regions (World, continents, income groups) come from the FAOSTAT garden step, which builds them
   for SCL as for every FAO dataset by summing member countries; FAO's own regional aggregates are dropped.

9. All stages are divided by the same population and by 365 days: OWID population for countries, and for regions the
   population of the members with data (the FAOSTAT garden step's own per-capita denominator), so that a region's
   per-capita values are not diluted by members that do not report. SCL covers 2010 onward.
"""

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

N_CHARACTERS_ITEM_CODE = 8

# SCL elements (garden element codes) and their short names in this step.
ELEMENTS = {
    "005510": "production",
    "005610": "imports",
    "005910": "exports",
    "005071": "stock_variation",
    "005525": "seed",
    "005016": "losses",
    "005165": "other_uses",
    "005023": "processing",
    "005520": "feed",
    "005164": "tourist_consumption",
    "005166": "residuals",
    "005141": "food",
    "000261": "food_kcal_per_year",
    "000271": "food_protein_tonnes_per_year",
}
ELEMENT_UNITS = {
    "kilocalories": ["000261"],
    "tonnes": [code for code in ELEMENTS if code != "000261"],
}
# FBS elements used for the fish items (see assumption 6).
FBS_ELEMENTS = {
    "005511": "production",
    "005611": "imports",
    "005911": "exports",
    "005301": "domestic_supply",
    "005527": "seed",
    "005123": "losses",
    "005154": "other_uses",
    "005131": "processing",
    "005521": "feed",
    "005171": "tourist_consumption",
    "005170": "residuals",
    "005142": "food",
    "0664pc": "food_kcal_per_capita_per_day",
    "0674pc": "food_protein_g_per_capita_per_day",
    "0645pc": "food_kg_per_capita_per_year",
}
FBS_PER_CAPITA_ELEMENTS = ["0664pc", "0674pc", "0645pc"]
# `numerator` is the food-nutrient column (kcal, or tonnes of protein); dividing it by food in tonnes and multiplying
# by `to_per_100g` gives the density per 100 g (kcal / tonnes -> kcal per 100 g is / 1e4; tonnes of protein / tonnes
# -> grams per 100 g is x 1e6 / 1e4).
NUTRIENTS = {
    "energy": {
        "numerator": "food_kcal_per_year",
        "to_per_100g": 1 / 1e4,
        "ceiling": 920,
        "unit": "kilocalories per person per day",
    },
    "protein": {
        "numerator": "food_protein_tonnes_per_year",
        "to_per_100g": 1e6 / 1e4,
        "ceiling": 100,
        "unit": "grams of protein per person per day",
    },
    "mass": {"numerator": None, "to_per_100g": None, "ceiling": None, "unit": "kilograms per person per day"},
}
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
ITEM_GROUP_ROLES = {
    "Crops, primary": "crop",
    "Livestock primary": "animal",
    "Crops processed": "processed",
    "Livestock processed": "processed",
}
ROLES = {"crop": "crop_production", "animal": "animal_products", "processed": "processed_production"}
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
FIRST_YEAR = 2010
# SCL carries population as an item; it is not a commodity.
POPULATION_ITEM_CODE = "00000001"
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# FBS "Grand Total" item, to compare our food stage with FAO's published food supply.
FBS_TOTAL_ITEM_CODE = "00002901"
HUNDRED_GRAMS_PER_TONNE = 10_000
KG_PER_TONNE = 1000
DAYS_PER_YEAR = 365
# Columns of the per-item balance table shared by the SCL and the FBS (fish) parts.
BALANCE_COLUMNS = (
    ["country", "year", "item_code", "fao_item", "role"]
    + BALANCE_ELEMENTS
    + ["food_kcal_per_year", "food_protein_tonnes_per_year", "food_tonnes_for_density"]
)


def _pad_code(code: int) -> str:
    return str(code).zfill(N_CHARACTERS_ITEM_CODE)


def load_manual_inputs() -> dict:
    with open(paths.side_file("food_supply_chain_scl.items.yml")) as f:
        config = yaml.safe_load(f)
    expected = {
        "fixed_densities",
        "never_food",
        "output_implied_densities",
        "role_overrides",
        "fish_from_fbs",
        "processing_families_for_checks",
    }
    assert set(config) == expected, f"Unexpected top-level keys in items file: {set(config) ^ expected}"
    for entry in config["fixed_densities"]["families"] + config["fixed_densities"]["items"]:
        assert {"energy", "protein"} <= set(entry), f"Fixed densities need energy and protein: {entry}"
    return config


def load_roles(tb_groups: Table) -> pd.Series:
    """Map each SCL item code to its role, from FAO's item groups."""
    groups = tb_groups[tb_groups["item_group"].astype(str) != "Grand Total"][["item_code", "item", "item_group"]].copy()
    groups["item_code"] = groups["item_code"].astype(int).map(_pad_code)
    groups["item_group"] = groups["item_group"].astype(str)
    assert not groups["item_code"].duplicated().any(), "An SCL item belongs to more than one FAO item group."
    unknown = set(groups["item_group"]) - set(ITEM_GROUP_ROLES)
    assert not unknown, f"Unknown FAO item groups in SCL: {unknown}"
    return groups.set_index("item_code")["item_group"].map(ITEM_GROUP_ROLES)


def sanity_check_inputs(tb: Table, roles: pd.Series, manual: dict) -> None:
    elements = tb[["element_code", "unit"]].drop_duplicates().set_index("element_code")["unit"].astype(str)
    for unit, codes in ELEMENT_UNITS.items():
        for code in codes:
            assert code in elements.index, f"Element {code} ({ELEMENTS[code]}) not found in SCL table."
            assert elements[code] == unit, f"Element {code} has unit {elements[code]!r}, expected {unit!r}."

    table_items = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"].astype(str)
    assert table_items.get(POPULATION_ITEM_CODE) == "Total population", "Population item not found in SCL."
    missing_role = sorted(set(table_items.index) - set(roles.index) - {POPULATION_ITEM_CODE})
    assert not missing_role, f"SCL items without an FAO item group: {[(c, table_items[c]) for c in missing_role]}"
    named = {}
    for item in manual["fixed_densities"]["items"] + manual["never_food"] + manual["role_overrides"]:
        named[item["code"]] = item["name"]
    for family in manual["output_implied_densities"]:
        named.update(family["crops"])
        named.update(family["products"])
        assert set(family["intermediates"]) <= set(family["products"]), "Intermediates must be among the products."
    wrong = {c: (n, table_items.get(_pad_code(c))) for c, n in named.items() if table_items.get(_pad_code(c)) != n}
    assert not wrong, f"Manual inputs name items that do not match SCL (code: (expected, found)): {wrong}"


def prepare_balance_table(tb: Table, roles: pd.Series, manual: dict) -> Table:
    """One row per (country, year, item) with one column per element, for all SCL items."""
    tb = tb[tb["element_code"].isin(ELEMENTS) & (tb["item_code"].astype(str) != POPULATION_ITEM_CODE)].reset_index(
        drop=True
    )
    tb = tb[["country", "year", "item_code", "fao_item", "element_code", "value", "population_with_data"]].astype(
        {"country": str, "item_code": str, "fao_item": str, "value": float, "population_with_data": float}
    )
    # Population of the entity, or, for a region, of its members with data (the FAOSTAT garden step's own per-capita
    # denominator). Taken as the maximum over items and elements, i.e. every member that reports anything.
    population = tb.groupby(["country", "year"])["population_with_data"].max().rename("population").reset_index()
    names = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"]
    tb = tb.drop(columns=["fao_item", "population_with_data"]).pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in ELEMENTS.items()})
    for element in ELEMENTS.values():
        if element not in tb.columns:
            tb[element] = np.nan
    tb[BALANCE_ELEMENTS] = tb[BALANCE_ELEMENTS].fillna(0)
    tb["fao_item"] = tb["item_code"].map(names)
    tb["role"] = tb["item_code"].map(roles)
    for override in manual["role_overrides"]:
        tb.loc[tb["item_code"] == _pad_code(override["code"]), "role"] = override["role"]
    assert tb["role"].notnull().all()
    tb["food_tonnes_for_density"] = tb["food"]
    tb = tb.merge(population, on=["country", "year"], how="left")
    assert tb["population"].notnull().all(), "Some SCL rows have no population."
    return tb[BALANCE_COLUMNS + ["population"]]


def prepare_fish_table(tb_fbsc: Table, manual: dict) -> Table:
    """FBS fish items, reshaped like the SCL balance table (see assumption 6)."""
    fish_items = {_pad_code(item["code"]): item for item in manual["fish_from_fbs"]}
    tb = tb_fbsc[
        tb_fbsc["item_code"].astype(str).isin(fish_items)
        & tb_fbsc["element_code"].astype(str).isin(FBS_ELEMENTS)
        & (tb_fbsc["year"] >= FIRST_YEAR)
    ].reset_index(drop=True)
    tb = tb[["country", "year", "item_code", "fao_item", "element_code", "value"]].astype(
        {"country": str, "item_code": str, "fao_item": str, "value": float}
    )
    found = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"]
    wrong = {c: (it["name"], found.get(c)) for c, it in fish_items.items() if found.get(c) != it["name"]}
    assert not wrong, f"FBS fish items do not match the items file (code: (expected, found)): {wrong}"

    tb = tb.drop(columns=["fao_item"]).pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in FBS_ELEMENTS.items()})
    for element in list(FBS_ELEMENTS.values()) + ["stock_variation"]:
        if element not in tb.columns:
            tb[element] = np.nan
    tonnes = [name for code, name in FBS_ELEMENTS.items() if code not in FBS_PER_CAPITA_ELEMENTS]
    tb[tonnes] = tb[tonnes].fillna(0)
    tb["stock_variation"] = tb["production"] + tb["imports"] - tb["exports"] - tb["domestic_supply"]
    tb["fao_item"] = tb["item_code"].map({c: it["name"] for c, it in fish_items.items()})
    tb["role"] = tb["item_code"].map({c: it["role"] for c, it in fish_items.items()})
    # FBS tonnages are rounded to 1,000 t, so densities are taken from its per-capita figures. The density is a
    # ratio, so the population cancels: express the per-capita figures per person in the same units as SCL's totals
    # (kcal per year; grams of protein / 1e6 -> tonnes; kg of food / 1000 -> tonnes).
    tb["food_kcal_per_year"] = tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR
    tb["food_protein_tonnes_per_year"] = tb["food_protein_g_per_capita_per_day"] * DAYS_PER_YEAR / 1e6
    tb["food_tonnes_for_density"] = tb["food_kg_per_capita_per_year"] / 1000
    tb["population"] = np.nan  # filled from the SCL rows of the same entity and year
    return tb[BALANCE_COLUMNS + ["population"]]


def sanity_check_balance_identity(tb: Table) -> None:
    supply = tb["production"] + tb["imports"] - tb["exports"] - tb["stock_variation"]
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * uses.abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_open = ((supply - uses).abs() > tolerance).mean()
    assert share_open < 0.01, f"Supply differs from the sum of uses in {100 * share_open:.2f}% of item balances."


def fixed_density_map(tb: Table, manual: dict, nutrient: str) -> dict[str, float]:
    """Fixed densities per item code for one nutrient, from the families (by name pattern) and the explicit items."""
    fixed = {}
    for family in manual["fixed_densities"]["families"]:
        for code, name in tb[["item_code", "fao_item"]].drop_duplicates().itertuples(index=False):
            if name.startswith(family["pattern"]):
                fixed[code] = family[nutrient]
    for item in manual["fixed_densities"]["items"]:
        fixed[_pad_code(item["code"])] = item[nutrient]
    return fixed


def add_densities(tb: Table, manual: dict, nutrient: str) -> Table:
    """Densities per (country, year, item) for one nutrient, following assumptions 3 to 5."""
    tb = tb.copy()
    config = NUTRIENTS[nutrient]
    if config["numerator"] is None:
        tb["density"] = KG_PER_TONNE / HUNDRED_GRAMS_PER_TONNE
        tb["density_source"] = "mass"
        return tb

    raw = config["to_per_100g"] * tb[config["numerator"]] / tb["food_tonnes_for_density"]
    raw = raw.where(np.isfinite(raw) & (raw >= 0))
    within_ceiling = raw.where(raw <= config["ceiling"])
    # A density of exactly zero is real for oils and sugars (no protein), but it is also what a tiny food quantity
    # rounded to zero produces, so zeros are not used directly: they enter the medians, which come out as zero
    # for items that truly have none of the nutrient and as the usual value otherwise.
    accepted = within_ceiling.where(within_ceiling > 0)
    country_median = within_ceiling.groupby([tb["country"], tb["item_code"]]).transform("median")
    item_median = within_ceiling.groupby(tb["item_code"]).transform("median")
    tb["density_raw"] = raw
    tb["density"] = accepted.fillna(country_median).fillna(item_median)
    tb["density_source"] = np.select(
        [accepted.notnull(), country_median.notnull(), item_median.notnull()],
        ["direct", "country_median", "item_median"],
        default="none",
    )

    # Assumption 4: items nobody eats, valued as human food; items that are never food, zero.
    fixed = fixed_density_map(tb, manual, nutrient)
    for item in manual["never_food"]:
        fixed[_pad_code(item["code"])] = 0.0
    no_density = tb["density"].isnull()
    tb.loc[no_density, "density"] = tb.loc[no_density, "item_code"].map(fixed)
    tb.loc[no_density & tb["density"].notnull(), "density_source"] = "fixed"
    never = tb["item_code"].isin([_pad_code(item["code"]) for item in manual["never_food"]])
    tb.loc[never, ["density", "density_source"]] = [0.0, "never_food"]

    # Assumption 5: crops not eaten as harvested get the density implied by their products, from World each year.
    world = tb[tb["country"] == "World"]
    for family in manual["output_implied_densities"]:
        crops = [_pad_code(c) for c in family["crops"]]
        products = [_pad_code(c) for c in family["products"]]
        intermediates = [_pad_code(c) for c in family["intermediates"]]
        w_products = world[world["item_code"].isin(products)]
        w_intermediates = world[world["item_code"].isin(intermediates)]
        w_crops = world[world["item_code"].isin(crops)]
        nutrient_out = (
            (w_products["production"] * HUNDRED_GRAMS_PER_TONNE * w_products["density"])
            .groupby(w_products["year"])
            .sum()
        )
        # Intermediate products (processed further within the family) would be counted twice, as their own
        # production and as the production of what they become; their processing is taken out.
        nutrient_in = (
            (w_intermediates["processing"] * HUNDRED_GRAMS_PER_TONNE * w_intermediates["density"])
            .groupby(w_intermediates["year"])
            .sum()
        )
        tonnes_in = w_crops["processing"].groupby(w_crops["year"]).sum()
        implied = (nutrient_out - nutrient_in.reindex(nutrient_out.index).fillna(0)) / (
            tonnes_in * HUNDRED_GRAMS_PER_TONNE
        )
        error = f"Implausible implied {nutrient} densities for {list(family['crops'].values())}: {implied.round(1).to_dict()}"
        assert implied.between(0, config["ceiling"]).all(), error
        mask = tb["item_code"].isin(crops)
        tb.loc[mask, "density"] = tb.loc[mask, "year"].map(implied)
        tb.loc[mask, "density_source"] = "implied_by_products"

    unresolved = tb[tb["density"].isnull() & (tb[BALANCE_ELEMENTS].abs().sum(axis=1) > 0)]
    error = f"Items with flows but no {nutrient} density (add them to the items file): " + str(
        unresolved.groupby("fao_item")["production"].sum().sort_values(ascending=False).round(0).to_dict()
    )
    assert unresolved.empty, error
    tb["density"] = tb["density"].fillna(0.0)
    return tb


def sanity_check_densities(tb: Table, nutrient: str) -> None:
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."
    mass = (tb["production"] + tb["imports"]).abs()
    provenance = mass.groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    log.info(f"food_supply_chain_scl.{nutrient}_density_provenance_pct_of_supply", **provenance.to_dict())
    # In SCL, staples such as paddy rice and sugar cane are only eaten after processing, so a large share of supply
    # legitimately uses median or product-implied densities rather than a direct one.
    assert provenance.get("none", 0) == 0, "Some supply has no density."
    assert provenance.get("direct", 0) > 55, (
        f"Only {provenance.get('direct', 0):.1f}% of supply uses a direct {nutrient} density."
    )
    summary = tb.groupby("fao_item").agg(
        role=("role", "first"),
        density_median=("density", "median"),
        source=("density_source", lambda x: x.value_counts().index[0]),
        production_mt=("production", lambda x: x.sum() / 1e6),
    )
    with pd.option_context("display.max_rows", None, "display.width", 200):
        log.info(
            f"food_supply_chain_scl.{nutrient}_densities_per_100g\n"
            + summary.sort_values("production_mt", ascending=False).round(1).to_string()
        )


def sanity_check_processing_families(tb: Table, manual: dict, nutrient: str) -> None:
    """For World in the latest year, nutrient out of processing as products vs nutrient in, per family."""
    world = tb[(tb["country"] == "World") & (tb["year"] == tb["year"].max())]
    for name, codes in manual["processing_families_for_checks"].items():
        members = world[world["item_code"].isin([_pad_code(c) for c in codes])]
        nutrient_in = (members["processing"] * members["density"]).sum()
        nutrient_out = (members.loc[members["role"] == "processed", "production"] * members["density"]).sum()
        ratio = nutrient_out / nutrient_in
        # Protein is lost more than energy in processing, because protein-rich by-products such as brewer's grains
        # are not SCL items; hence the looser band.
        low, high = (0.75, 1.15) if nutrient == "energy" else (0.5, 1.2)
        assert low < ratio < high, f"Processing family {name!r} ({nutrient}): out is {100 * ratio:.0f}% of in."


def build_chain(tb: Table) -> Table:
    converted = tb[["country", "year"]].copy()
    for element in BALANCE_ELEMENTS:
        converted[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["density"]
    for role, stage in ROLES.items():
        converted[stage] = converted["production"].where(tb["role"] == role, 0)
    converted = converted.drop(columns=["production"])
    chain = converted.groupby(["country", "year"], as_index=False).sum(min_count=1)
    chain = chain.merge(tb[["country", "year", "population"]].drop_duplicates(), on=["country", "year"], how="left")

    chain["processing_net"] = chain["processing"] - chain["processed_production"]
    chain = chain.drop(columns=["processing", "processed_production"])

    chain_end = chain["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - chain[stage] if stage in SUBTRACTED_STAGES else chain_end + chain[stage]
    chain["balancing_difference"] = chain["food"] - chain_end
    chain["residuals"] = chain["residuals"] - chain["balancing_difference"]

    # Per person per day (assumption 9): OWID population, or for regions the population of the members with data.
    assert chain["population"].notnull().all() and (chain["population"] > 0).all(), "Missing population."
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    return chain[["country", "year"] + STAGES]


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
    chain_end = tb["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - tb[stage] if stage in SUBTRACTED_STAGES else chain_end + tb[stage]
    assert (chain_end - tb["food"]).abs().max() < 1e-3 * tb["food"].abs().max(), "Chain does not land on food."

    world = tb[tb["country"] == "World"].set_index("year")
    # Our food stage against FAO's published food supply (the FBS "Grand Total"), for World.
    fao_element = {"energy": "0664pc", "protein": "0674pc"}.get(nutrient)
    if fao_element is None:
        return
    fao_total = (
        tb_fbsc[
            (tb_fbsc["country"].astype(str) == "World")
            & (tb_fbsc["element_code"].astype(str) == fao_element)
            & (tb_fbsc["item_code"].astype(str) == FBS_TOTAL_ITEM_CODE)
        ]
        .set_index("year")["value"]
        .astype(float)
    )
    deviation = (world["food"] - fao_total).dropna() / fao_total
    log.info(
        f"food_supply_chain_scl.{nutrient}_food_vs_fao_total",
        max_deviation_pct=round(100 * deviation.abs().max(), 2),
        latest_year=int(deviation.index.max()),
        latest_deviation_pct=round(100 * deviation[deviation.index.max()], 2),
    )
    assert deviation.abs().max() < 0.05, (
        f"World food {nutrient} deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    )
    world_processing_share = (world["processing_net"] / world["crop_production"]).abs().max()
    assert world_processing_share < 0.1, (
        f"Net processing is {100 * world_processing_share:.0f}% of crop production for World ({nutrient})."
    )


def run() -> None:
    #
    # Load inputs.
    #
    ds_scl = paths.load_dataset("faostat_scl")
    tb_scl = ds_scl.read("faostat_scl", safe_types=False)
    ds_metadata = paths.load_dataset("faostat_metadata")
    tb_groups = ds_metadata.read("faostat_scl_item_group", safe_types=False)
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc", safe_types=False)
    manual = load_manual_inputs()

    #
    # Process data.
    #
    tb_scl = tb_scl[~tb_scl["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    tb_fbsc = tb_fbsc[~tb_fbsc["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    roles = load_roles(tb_groups)
    sanity_check_inputs(tb_scl, roles=roles, manual=manual)

    tb = prepare_balance_table(tb_scl, roles=roles, manual=manual)
    tb_fish = prepare_fish_table(tb_fbsc, manual=manual)
    # Only for the entities and years SCL covers; FBS also has OWID region aggregates, which SCL does not.
    covered = tb[["country", "year"]].drop_duplicates()
    tb_fish = tb_fish.merge(covered, on=["country", "year"], how="inner")
    tb = pr.concat([tb, tb_fish], ignore_index=True)
    tb["population"] = tb.groupby(["country", "year"])["population"].transform("max")
    sanity_check_balance_identity(tb)

    tables = []
    for nutrient in NUTRIENTS:
        tb_nutrient = add_densities(tb, manual=manual, nutrient=nutrient)
        if nutrient != "mass":
            sanity_check_densities(tb_nutrient, nutrient=nutrient)
            sanity_check_processing_families(tb_nutrient, manual=manual, nutrient=nutrient)
        chain = build_chain(tb_nutrient)
        sanity_check_outputs(chain, tb_fbsc=tb_fbsc, nutrient=nutrient)
        tables.append(chain.format(["country", "year"], short_name=nutrient))

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
