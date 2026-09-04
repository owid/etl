"""FAO Supply Utilization Accounts (SCL) as a chain of stages in kilocalories per person per day.

Same chain as `food_supply_chain_fbs` (crop production, trade, stock changes, seed, losses, other uses, processing,
feed, animal products, food), built from the Supply Utilization Accounts instead of the Food Balance Sheets. SCL
reports individual commodities rather than FBS's item groups, and it includes the by-products that FBS leaves out:
oilseed cakes, brans, gluten feed. With those items present, the calories that go from oilseeds to cakes to animals
appear under feed, where they belong, instead of vanishing inside processing.

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
                  that the bar shows only the calories that go into a factory and do not come out as a product.
   One override, listed in the items file: cotton seed is treated as a crop, because the seed cotton it is ginned
   from is a fibre crop and not in SCL.

3. Energy density of an item (kcal per 100 g) = food supply in kcal per person per day / food supply in grams per
   person per day x 100, per item, country and year. The same density is applied to every flow of the item, so
   the identity holds in kcal exactly as in tonnes and the chain closes by construction. The only rejection rule is
   a physical ceiling (nothing edible has more energy than pure fat, FAT_CEILING_KCAL_PER_100G); rejected cells fall
   back to the country's median for the item over all years, then to the item's median over all countries and years.

4. Items nobody eats (cakes, brans, ethanol, refining residues) have no food figures to reverse-engineer from. They
   get the density of the human food they would be if eaten, from USDA's food composition tables: numbers and
   sources are in `food_supply_chain_scl.items.yml`. Items that are never food (castor, tung, kapok, jojoba, wool
   grease) get zero, which removes them from every flow consistently.

5. Crops that are not eaten as harvested (paddy rice, sugar cane and beet, oil palm fruit, rapeseed, cotton seed) have a
   food-based density that rests on a sliver of the crop, or none at all, and it is far below the products they
   yield. Their density is derived from those products instead: calories of the family's products, minus those of
   intermediate products processed further within the family, over the tonnes of crop that went into processing,
   for World each year, applied to every country. The crop-to-product links are listed in the items file.

6. Fish and seafood are not in SCL. The FBS fish items are spliced in, with their FBS densities under the same rules.

7. FAOSTAT rounds tonnages, so balances do not close exactly. The gap is folded into "residuals" so the chain lands
   exactly on "food"; its size is kept in "balancing_difference".

8. All stages are divided by the same population, from OWID's population dataset, and by 365 days. SCL covers 2010
   onward, countries plus World and the European Union (27); FAO's own regional aggregates are dropped.
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
    "000664": "food_kcal_per_capita_per_day",
    "000665": "food_g_per_capita_per_day",
}
ELEMENT_UNITS = {
    "Kilocalories per capita per day": ["000664"],
    "Grams per capita per day": ["000665"],
    "Tonnes": [code for code in ELEMENTS if not code.startswith("0006")],
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
    "0645pc": "food_kg_per_capita_per_year",
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
FAT_CEILING_KCAL_PER_100G = 920
# Tolerances for the identity check in tonnes (relative to the item's uses, plus FAOSTAT's rounding).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# FBS "Grand Total" item, to compare our food stage with FAO's published food supply.
FBS_TOTAL_ITEM_CODE = "00002901"
HUNDRED_GRAMS_PER_TONNE = 10_000
DAYS_PER_YEAR = 365


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

    # Every item in the data has a role, and every item named in the manual inputs exists with that name.
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
    tb = tb[["country", "year", "item_code", "fao_item", "element_code", "value"]].astype(
        {"country": str, "item_code": str, "fao_item": str, "value": float}
    )
    names = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"]
    tb = tb.drop(columns=["fao_item"]).pivot(
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
    # Food-based density, in kcal per 100 g.
    tb["density_raw"] = 100 * tb["food_kcal_per_capita_per_day"] / tb["food_g_per_capita_per_day"]
    return tb


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
    tonnes = [c for c in FBS_ELEMENTS.values() if "capita" not in c]
    tb[tonnes] = tb[tonnes].fillna(0)
    tb["stock_variation"] = tb["production"] + tb["imports"] - tb["exports"] - tb["domestic_supply"]
    tb["fao_item"] = tb["item_code"].map({c: it["name"] for c, it in fish_items.items()})
    tb["role"] = tb["item_code"].map({c: it["role"] for c, it in fish_items.items()})
    tb["density_raw"] = tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR / (tb["food_kg_per_capita_per_year"] * 10)
    return tb[["country", "year", "item_code", "fao_item", "role", "density_raw"] + BALANCE_ELEMENTS]


def sanity_check_balance_identity(tb: Table) -> None:
    supply = tb["production"] + tb["imports"] - tb["exports"] - tb["stock_variation"]
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * uses.abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_open = ((supply - uses).abs() > tolerance).mean()
    assert share_open < 0.01, f"Supply differs from the sum of uses in {100 * share_open:.2f}% of item balances."


def add_energy_densities(tb: Table, manual: dict) -> Table:
    """Densities per (country, year, item), following assumptions 3 to 5."""
    raw = tb["density_raw"].where(np.isfinite(tb["density_raw"]) & (tb["density_raw"] > 0))
    accepted = raw.where(raw <= FAT_CEILING_KCAL_PER_100G)
    country_median = accepted.groupby([tb["country"], tb["item_code"]]).transform("median")
    item_median = accepted.groupby(tb["item_code"]).transform("median")
    tb["density"] = accepted.fillna(country_median).fillna(item_median)
    tb["density_source"] = np.select(
        [accepted.notnull(), country_median.notnull(), item_median.notnull()],
        ["direct", "country_median", "item_median"],
        default="none",
    )

    # Assumption 4: items nobody eats, valued as human food; items that are never food, zero.
    fixed = {}
    for family in manual["fixed_densities"]["families"]:
        for code, name in tb[["item_code", "fao_item"]].drop_duplicates().itertuples(index=False):
            if name.startswith(family["pattern"]):
                fixed[code] = family["density"]
    for item in manual["fixed_densities"]["items"]:
        fixed[_pad_code(item["code"])] = item["density"]
    for item in manual["never_food"]:
        fixed[_pad_code(item["code"])] = 0.0
    no_density = tb["density"].isnull()
    tb.loc[no_density, "density"] = tb.loc[no_density, "item_code"].map(fixed)
    tb.loc[no_density & tb["density"].notnull(), "density_source"] = "fixed"
    # Never-food items are zeroed even where some country reports food for them.
    never = tb["item_code"].isin([_pad_code(item["code"]) for item in manual["never_food"]])
    tb.loc[never, ["density", "density_source"]] = [0.0, "never_food"]

    # Assumption 5: crops not eaten as harvested get the density implied by their products, from World each year.
    world = tb[tb["country"] == "World"]
    for family in manual["output_implied_densities"]:
        crops = [_pad_code(c) for c in family["crops"]]
        products = [_pad_code(c) for c in family["products"]]
        w_products = world[world["item_code"].isin(products)]
        w_crops = world[world["item_code"].isin(crops)]
        kcal_out = (
            (w_products["production"] * HUNDRED_GRAMS_PER_TONNE * w_products["density"])
            .groupby(w_products["year"])
            .sum()
        )
        # Intermediate products (processed further within the family) would be counted twice, as their own
        # production and as the production of what they become; their processing is taken out.
        intermediates = [_pad_code(c) for c in family["intermediates"]]
        w_intermediates = world[world["item_code"].isin(intermediates)]
        kcal_in = (
            (w_intermediates["processing"] * HUNDRED_GRAMS_PER_TONNE * w_intermediates["density"])
            .groupby(w_intermediates["year"])
            .sum()
        )
        tonnes_in = w_crops["processing"].groupby(w_crops["year"]).sum()
        implied = (kcal_out - kcal_in.reindex(kcal_out.index).fillna(0)) / (tonnes_in * HUNDRED_GRAMS_PER_TONNE)
        error = f"Implausible implied densities for {list(family['crops'].values())}: {implied.round(0).to_dict()}"
        assert implied.between(10, FAT_CEILING_KCAL_PER_100G).all(), error
        mask = tb["item_code"].isin(crops)
        tb.loc[mask, "density"] = tb.loc[mask, "year"].map(implied)
        tb.loc[mask, "density_source"] = "implied_by_products"

    unresolved = tb[tb["density"].isnull() & (tb[BALANCE_ELEMENTS].abs().sum(axis=1) > 0)]
    error = "Items with flows but no density (add them to the items file): " + str(
        unresolved.groupby("fao_item")["production"].sum().sort_values(ascending=False).round(0).to_dict()
    )
    assert unresolved.empty, error
    tb["density"] = tb["density"].fillna(0.0)
    return tb


def sanity_check_energy_densities(tb: Table) -> None:
    assert (tb["density"] >= 0).all() and (tb["density"] <= FAT_CEILING_KCAL_PER_100G).all(), "Densities out of range."
    mass = (tb["production"] + tb["imports"]).abs()
    provenance = mass.groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    log.info("food_supply_chain_scl.density_provenance_pct_of_supply", **provenance.to_dict())
    # In SCL, staples such as paddy rice and sugar cane are only eaten after processing, so a large share of supply
    # legitimately uses median or product-implied densities rather than a direct one.
    assert provenance.get("none", 0) == 0, "Some supply has no density."
    assert provenance.get("direct", 0) > 55, f"Only {provenance.get('direct', 0):.1f}% of supply uses a direct density."
    summary = tb.groupby("fao_item").agg(
        role=("role", "first"),
        density_median=("density", "median"),
        source=("density_source", lambda x: x.value_counts().index[0]),
        production_mt=("production", lambda x: x.sum() / 1e6),
    )
    with pd.option_context("display.max_rows", None, "display.width", 200):
        log.info(
            "food_supply_chain_scl.energy_densities_kcal_per_100g\n"
            + summary.sort_values("production_mt", ascending=False).round(1).to_string()
        )


def build_chain(tb: Table) -> Table:
    kcal = tb[["country", "year"]].copy()
    for element in BALANCE_ELEMENTS:
        kcal[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["density"]
    for role, stage in ROLES.items():
        kcal[stage] = kcal["production"].where(tb["role"] == role, 0)
    kcal = kcal.drop(columns=["production"])
    chain = kcal.groupby(["country", "year"], as_index=False).sum(min_count=1)

    chain["processing_net"] = chain["processing"] - chain["processed_production"]
    chain = chain.drop(columns=["processing", "processed_production"])

    chain_end = chain["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - chain[stage] if stage in SUBTRACTED_STAGES else chain_end + chain[stage]
    chain["balancing_difference"] = chain["food"] - chain_end
    chain["residuals"] = chain["residuals"] - chain["balancing_difference"]

    chain = paths.regions.add_population(chain, warn_on_missing_countries=False)
    missing_population = sorted(set(chain[chain["population"].isnull()]["country"]))
    if missing_population:
        log.warning(f"Dropping entities without OWID population: {missing_population}")
        chain = chain.dropna(subset=["population"]).reset_index(drop=True)
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    return chain[["country", "year"] + STAGES]


def sanity_check_processing_families(tb: Table, manual: dict) -> None:
    """For World in the latest year, calories out of processing as products vs calories in, per family."""
    world = tb[(tb["country"] == "World") & (tb["year"] == tb["year"].max())]
    for name, codes in manual["processing_families_for_checks"].items():
        members = world[world["item_code"].isin([_pad_code(c) for c in codes])]
        kcal_in = (members["processing"] * members["density"]).sum()
        kcal_out = (members.loc[members["role"] == "processed", "production"] * members["density"]).sum()
        ratio = kcal_out / kcal_in
        assert 0.8 < ratio < 1.1, f"Processing family {name!r}: calories out are {100 * ratio:.0f}% of calories in."


def sanity_check_outputs(tb: Table, tb_fbsc: Table) -> None:
    assert tb.columns[tb.isna().all()].empty, "Output has fully-nan columns."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    for stage in [
        s for s in STAGES if s not in ["stock_variation", "residuals", "processing_net", "balancing_difference"]
    ]:
        assert (tb[stage].fillna(0) >= 0).all(), f"Negative values in stage {stage!r}."
    chain_end = tb["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - tb[stage] if stage in SUBTRACTED_STAGES else chain_end + tb[stage]
    assert (chain_end - tb["food"]).abs().max() < 1e-3, "Chain does not land on food."

    # Our food stage against FAO's published food supply (the FBS "Grand Total"), for World.
    world = tb[tb["country"] == "World"].set_index("year")
    fao_total = (
        tb_fbsc[
            (tb_fbsc["country"].astype(str) == "World")
            & (tb_fbsc["element_code"].astype(str) == "0664pc")
            & (tb_fbsc["item_code"].astype(str) == FBS_TOTAL_ITEM_CODE)
        ]
        .set_index("year")["value"]
        .astype(float)
    )
    deviation = (world["food"] - fao_total).dropna() / fao_total
    log.info(
        "food_supply_chain_scl.food_vs_fao_total",
        max_deviation_pct=round(100 * deviation.abs().max(), 2),
        latest_year=int(deviation.index.max()),
        latest_deviation_pct=round(100 * deviation[deviation.index.max()], 2),
    )
    assert deviation.abs().max() < 0.03, (
        f"World food supply deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    )
    world_processing_share = (world["processing_net"] / world["crop_production"]).abs().max()
    assert world_processing_share < 0.1, (
        f"Net processing is {100 * world_processing_share:.0f}% of crop production for World."
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
    tb = pr.concat([tb[tb_fish.columns], tb_fish], ignore_index=True)
    sanity_check_balance_identity(tb)
    tb = add_energy_densities(tb, manual=manual)
    sanity_check_energy_densities(tb)
    sanity_check_processing_families(tb, manual=manual)
    chain = build_chain(tb)
    sanity_check_outputs(chain, tb_fbsc=tb_fbsc)

    chain = chain.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[chain])
    ds_garden.save()
