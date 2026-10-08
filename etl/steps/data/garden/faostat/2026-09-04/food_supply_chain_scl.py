"""FAO Supply Utilization Accounts (SCL) as a chain of stages from crop production to food, per person per day.

This step builds the same chain as `food_supply_chain_fbs`, but from FAO's Supply Utilization Accounts (SCL)
instead of the Food Balance Sheets (FBS): crop production, plus imports, minus exports, stock changes, seed,
losses, non-food uses, processing and animal feed, plus animal products, ending at the food available to eat.
Food keeps its reported calories and protein where the directly derived density is accepted and not overridden;
otherwise it uses the replacement density. The chain is built three times, in three units, one table each:
    energy   kilocalories per person per day
    protein  grams of protein per person per day
    mass     kilograms per person per day (the balance in tonnes, with no conversion at all)

Two differences make SCL preferable to FBS for this chain:
- SCL reports individual commodities, while FBS reports groups. SCL has "Wheat", "Wheat and meslin flour" and
  "Bread" as three separate items; FBS merges all three into one item, "Wheat and products".
- SCL includes the by-products that FBS has no items for: oilseed cakes, brans, gluten feed. When oilseeds
  (soybeans, rapeseed, sunflower seeds) are crushed to extract their oil, the crushed solids that remain are called
  cake. Cake is rich in protein, and it is one of the main things the world feeds to its farm animals. With the
  cakes present as items, what goes from oilseeds to cakes to animals appears under feed, where it belongs; in FBS
  the oilseeds that were sent to be crushed are counted under processing and the cake is never counted anywhere.
The price is coverage: SCL starts in 2010 (FBS starts in 1961) and misses a few countries that FAO has not
compiled (Japan, Sudan, Somalia and a few others).

ASSUMPTIONS THAT GO INTO THE CALCULATION
-----------------------------------------
1. The balance identity.
   >> Scale: major. The identity is the backbone of every table.

   For every item, country and year, SCL reports, in tonnes:
       production + imports - exports - stock variation
         = food + feed + seed + processing + other uses + losses + tourist consumption + residuals.
   Unlike FBS, SCL reports stock variation for every year, so nothing needs to be derived. The identity is checked
   item by item, not assumed.

2. Missing elements are treated as zero.
   >> Scale: minor. Bookkeeping that changes no values; it only lets the identity be evaluated for every item.

   When SCL does not report an element for an item, the step treats the missing element as zero, so that the
   identity can be evaluated for every item. This is safe for two reasons. First, FAO builds each balance as a
   whole, estimating every element of the item together, so an element missing from a compiled balance is almost
   always one that does not apply to the item, not lost data. Second, if an element with a real value were missing
   and treated as zero, the two sides of the identity would not close for that item, and the step fails when more
   than 1% of the item balances do not close.

3. Densities.
   >> Scale: major. These densities convert all stages other than directly reported food into calories and protein.

   SCL reports every element of the balance only in tonnes. The exception is food: for each item, country and year,
   SCL also reports the food as calories (the element "Calories/Year") and as protein (the element "Proteins/Year").
   To build the chain in calories and protein, every other element must be converted from tonnes, and the food
   element is the only place the conversion factor can come from. The density of an item (kcal, or grams of
   protein, per 100 g) is the food nutrient per year divided by the food tonnes per year. That density, derived
   from the food element alone, is then used to convert the other elements of the item into the nutrient: production,
   imports, exports, stock variation, seed, losses, other uses, processing, feed, tourist consumption and
   residuals. Where this directly derived density is accepted and not overridden under assumptions 4 to 7, food
   keeps its reported nutrient total. This matters for fish taken from FBS (assumption 9), whose food tonnage is
   rounded more coarsely than the figures used to derive its density. Where a density is replaced, food continues
   to use that replacement density and the food tonnage. Any gap between the converted stages and food goes to
   data adjustments (assumption 10).
   FAO computes the food nutrients with one fixed factor per item: an item's density is the same in every country
   and year (for example 334 kcal per 100 g for wheat grain, 348 for maize, 64 for potatoes). Nine pairs of country
   and item are the only exceptions, such as milled rice in Bangladesh (360 kcal per 100 g instead of 349). A check
   fails the step if this ever changes.
   Besides calories and protein, the chain is also built in a third unit, mass. The mass table involves no density
   calculation: SCL already reports every element as a mass, and the step only changes the unit from tonnes to
   kilograms.

4. Rejected and missing densities, and their fallbacks.
   >> Scale: minor. Densities derived directly from the data cover about 66% of tonnage in energy (63% in protein);
   the medians cover about 8% (12% in protein). The rest uses the fixed and the product-implied densities of
   assumptions 6 and 7.

   A density is rejected only when it is physically impossible: more energy than pure fat (920 kcal per 100 g), or
   more than 100 g of protein per 100 g. Densities that are unusual but physically possible are kept as they are.
   A density can also be missing entirely, when the item is not eaten in that country.
   In both cases the step falls back to the country's median density for the item over all years, and then to the
   item's median over all countries and years.

5. Densities of exactly zero.
   >> Scale: minor. A zero reported nutrient gives a raw zero in 0.1% of cells in energy and 6.6% in protein; about
   one zero in nine is spurious and gets a nonzero value from the medians.

   A derived density of exactly zero gets a special treatment, because a zero can mean two things:
   - A true zero: the item has none of the nutrient. Sugar and oils contain no protein, and FAO's own figures give
     them a density of zero in every country and year.
   - A spurious zero: a small reported nutrient amount was rounded down to zero despite a positive food quantity,
     for an item that normally has a nonzero density.
   To handle both correctly, the step never uses a zero directly. When the division gives exactly zero, the cell
   takes the median density instead (the country's median for the item over all years, then the item's median over
   all countries and years), and the zeros are included in those medians. For a true zero, every year is zero, so
   the median is zero and the item correctly ends at zero. For a spurious zero, the median is the item's usual
   value, so the cell gets that.

6. Items nobody eats get fixed densities.
   >> Scale: major for the stages "feed" and "processing_net". The fixed densities carry only 3.4% of tonnage, but
   that tonnage is the oilseed cakes and brans whose presence is the main reason to use SCL at all.

   Items nobody eats (cakes, brans, ethanol, refining residues) have no food use, so no density can be derived from
   the data. Each gets the energy and protein of the human food it would be if eaten: energy from USDA's food
   composition tables, protein of each cake from the Feedipedia feed tables. The values and sources are in
   `food_supply_chain_scl.items.yml`.
   These fixed densities only apply to an item when less than 1% of the item's supply is eaten as food. The reason
   is that a few hundred tonnes of soybean cake eaten somewhere would otherwise set the density of hundreds of
   millions of tonnes of cake. An item on the list with a real food use (spirits, and wheat bran in some countries)
   keeps its data-derived density instead.
   Items that are never food in any form (castor, tung, kapok, jojoba, wool grease) get zero energy and protein,
   which removes them from every element consistently. In the mass table every item counts as it is.

7. Crops that are not eaten as harvested get densities implied by their products.
   >> Scale: major for the stage "crop_production". The product-implied densities carry 22% of tonnage: paddy rice,
   sugar cane, sugar beet, oil palm fruit, rapeseed and cotton seed.

   A density derived from food use measures the calories a human gets from eating the item; for these crops that is
   the wrong measure, because almost the whole harvest goes to factories, and a factory extracts far more than a
   human eating the crop raw (most of a chewed cane stalk is spat out; a mill takes nearly all of the sugar). The
   density of these crops is therefore derived from the products made out of them: the calories in the family's
   products, minus the calories of intermediate products processed further within the same family, divided by the
   tonnes of the crop that went into processing. The ratio is computed for World each year and applied to every
   country. The crop-to-product links are in the items file.

8. Roles: where each item's production enters the chain.
   >> Scale: major. Together with the densities, the roles are the shape of the chain: they decide which stage
   every tonne of production lands in.

   Adding up the production of all items would count the same calories many times, because sugar is made from sugar
   cane and bread is made from wheat. To avoid double counting, each item has a role, and the role decides where the
   production of the item goes. FAO's own item groups assign every SCL item to one of four groups ("Crops,
   primary", "Livestock primary", "Crops processed" or "Livestock processed"), which map to the three roles:
   - "crop": the production of crops is the stage "crop_production", the start of the chain.
   - "animal": animals eat crops, and the stage "feed" subtracts the calories of the crops that are fed to animals.
     The stage "animal_products" then adds the calories of the meat, milk and eggs that the animals produce.
     The production of animal items is that second stage.
   - "processed": factories turn crops into sugar, oils, flour and other products. The stage "processing_net" is
     the "processing" of all items (what is sent into factories) minus the "production" of processed items (what
     comes out of factories as food), so the stage counts only the calories that enter factories and do not come
     back as food.
   One override, listed in the items file: cotton seed is treated as a crop. Cotton seed comes out of ginning seed
   cotton, but seed cotton is a fibre crop and not an SCL item, so cotton seed is the first point in SCL where the
   calories of the cotton harvest exist.

9. Fish and seafood are taken from FBS.
   >> Scale: minor for World (fish production is 46 of the 631 kcal per person per day of animal products in 2023,
   and 6 of the 43 g of protein); large for fishing nations.

   Fish and seafood are not in SCL. The FBS fish items are added, with their FBS densities under the same rules,
   for the countries and years that SCL covers.

10. The gap between the preceding stages and food goes to data adjustments.
    >> Scale: minor for World; rounded fish tonnages can have a larger effect in small countries.

    FAOSTAT rounds tonnages, so balances do not close exactly. Food nutrients are retained at their reported
    precision where the direct density is accepted; the other stages still use the reported tonnages. This
    matters especially for fish taken from FBS. The gap between those stages and food is included in
    "data_adjustments", together with FAO's own "residuals", so that the chain ends exactly on "food".
    Keeping the more precise food total changes this adjustment without changing the other flows. The size of
    the gap is kept in "balancing_difference" for quality control.

11. World exports are set equal to World imports, and World tourist consumption is set to zero.
    >> Scale: minor, and only for World: the trade gap is a few percent of imports, and World tourist
    consumption is at most 0.21% of food.

    The world as a whole does not trade with anyone, so World imports and World exports should be equal. In the
    data they differ, because each is the sum of what individual countries report. World exports are set equal to
    World imports, which FAO considers the better-documented side (FAO 2025, Food Balance Sheets and Supply
    Utilization Accounts Resource Handbook, section 6.1), and the difference goes to "data_adjustments". Other
    regions do trade with the rest of the world and are left as they are.

    World tourist consumption has the same problem as World trade. "tourist_consumption" is food eaten in a
    country by people who live in another country. Every visitor lives in some country, so the world as a whole
    has no visitors, and World tourist consumption should be zero. In the data it is not zero: from 2010 (the
    first year FAO reports tourist consumption) it is 1 to 6 kcal per person per day, which is 0.05% to 0.21% of
    World food. World tourist consumption is set to zero, and the removed amount goes to "data_adjustments".
    Countries and other regions keep their tourist consumption, because for them food eaten by visitors is real.

12. Regions.
    >> Scale: major for the 10 region aggregates, which only exist through this assumption; no country's values
    change.

    FAO's own regional aggregates are dropped, and OWID regions (World, continents, income groups) are built from
    the member countries that have an SCL balance that year: every element and every food nutrient total is summed
    over those countries (fish included, from the same countries), and the region's population is the sum of those
    same countries' population. A country either has a full balance or none at all, so the elements and the
    population always cover the same countries.

13. Low-coverage region-years are dropped, and "Low-income countries" is left out.
    >> Scale: minor. It removes a few region-years and one region; no country's values change.

    A region-year is dropped when the countries with a balance hold less than 80% of the region's population, so
    that a value labeled "Africa" is never built from a small fraction of Africa. "Low-income countries" is left
    out altogether: SCL never covers more than 80% of its population (Japan, Sudan, Somalia and a few others are
    not compiled).

14. All stages are divided by that population and by 365 days, to give values per person per day.
    >> Scale: minor. A choice of unit, not of substance; it rescales all values equally.

KNOWN PROBLEMS, NOT YET RESOLVED
--------------------------------
- Processing appears to create calories in some countries.
  >> Scale: major for Brazil, about -650 kcal per person per day in 2023; small for World. Not fixed yet; the FBS
  sibling has the same symptom with a different cause.

  In the normal case "processing_net" is positive: factories lose some calories. But in Brazil, "processing_net" is
  negative, as if Brazilian factories created calories. Two inconsistencies cause this:
  - Ethanol. The production of ethanol counts as factory output, valued at 700 kcal per 100 g (assumption 6). But
    ethanol is not among the products used to derive the density of sugar cane (assumption 7), so the cane entering
    the mills is never credited with the calories of the ethanol made from it. Calories come out that were never
    counted going in. Brazil, where a large share of the cane becomes ethanol, is the extreme case.
  - The soy family. Soybeans enter processing at their data-derived density (406 kcal per 100 g in Brazil), but the
    outputs, cake at the fixed 330 (assumption 6) plus oil at 900, add up to about 6% more than the beans carried.
    Brazil crushes so much soy that 6% is large.

- The product-implied densities are World-level ratios applied to every country.
  >> Scale: minor for World by construction; it distorts countries whose product mix differs from the world
  average (again Brazil, with its ethanol).

  A country whose product mix differs from the world average gets a density of assumption 7 that does not match
  what its own factories make.

- The stage "feed" only counts feed that passes through the balance.
  >> Scale: "animal_products" exceeds "feed" in 13% of country-years in energy and 22% in protein, mostly real free
  inputs rather than errors; World is unaffected (feed 2,084 against 631 kcal in 2023, and 106 against 43 g of
  protein).

  Grass, pasture and forage are not SCL items, so everything grazing animals eat from pasture enters the chain
  nowhere, and wild-caught fish count as animal products with no feed at all. For fishing and grazing countries
  (Iceland, Mongolia, several island states), "animal_products" can therefore exceed "feed".
"""

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

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
    "million Kilocalories": ["000261"],
    "Tonnes": [code for code in ELEMENTS if code != "000261"],
}
KCAL_PER_MILLION_KCAL = 1e6
# FBS elements used for the fish items (see assumption 9).
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
# SCL carries population as an item; it is not a commodity, so it is excluded from the balance table.
POPULATION_ITEM_CODE = "00000001"
# FBS "Grand Total" item, used only to compare our food stage with FAO's own published World food supply.
FBS_TOTAL_ITEM_CODE = "00002901"
# `numerator` is the column holding the food nutrient total per year, already in the density's own unit
# (kilocalories, or grams of protein); dividing it by the number of 100 g portions of food eaten per year gives the
# density per 100 g. `ceiling` is the physical maximum density. Mass needs neither.
NUTRIENTS = {
    "energy": {"numerator": "food_kcal_per_year", "ceiling": 920, "unit": "kilocalories per person per day"},
    "protein": {"numerator": "food_protein_g_per_year", "ceiling": 100, "unit": "grams of protein per person per day"},
    "mass": {"numerator": None, "ceiling": None, "unit": "kilograms per person per day"},
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
# The three item roles of assumption 8. The production of crops starts the chain as "crop_production", the
# production of animal items is added back as "animal_products", and the production of processed items is netted
# against "processing" to give "processing_net".
ROLES = {"crop", "animal", "processed"}
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
    "data_adjustments",
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
    "data_adjustments",
]
# Unit conversions. Exact by definition; they change no data beyond the choice of unit (assumption 14).
HUNDRED_GRAMS_PER_TONNE = 10_000
KG_PER_TONNE = 1000
GRAMS_PER_TONNE = 1_000_000
DAYS_PER_YEAR = 365

# SCL covers 2010 onward; the FBS fish items of assumption 9 are cut to the same years.
FIRST_YEAR = 2010

# Checks only; these thresholds change no data, they only decide when the step crashes.
# Tolerance of the identity check, per item balance: 1% of the summed uses plus FAO's rounding (checks
# assumptions 1 and 2).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# Expected outcome of the coverage rule below; empty, because "Low-income countries" is left out altogether
# (checks assumption 13).
REGIONS_WITH_LOW_COVERAGE = set()
# Safety limit for assumption 11: World trade is only equalized while the gap is small; a gap above this share
# of imports crashes the step instead.
MAX_WORLD_TRADE_GAP = 0.2
# Safety limit for assumption 11: World tourist consumption is only set to zero while it is small (at most 0.21%
# of food in practice); a larger share of food crashes the step instead.
MAX_WORLD_TOURIST_CONSUMPTION = 0.005
# FAO uses one calorie factor and one protein factor per item, the same in every country and year (checks
# assumption 3). These are the only countries where FAO uses a factor of its own for an item.
COUNTRY_SPECIFIC_NUTRIENT_FACTORS = {
    ("Afghanistan", "Wheat and meslin flour"),
    ("Bangladesh", "Rice, milled"),
    ("Ethiopia", "Other vegetables provisionally preserved"),
    ("Namibia", "Skim milk of cows"),
    ("North Korea", "Rice, milled"),
    ("Peru", "Cassava, fresh"),
    ("Peru", "Meat of chickens, fresh or chilled"),
    ("Peru", "Potatoes"),
    ("Peru", "Rice, milled"),
}
# The factor check only uses food quantities of at least 1,000 tonnes; below that, FAO's rounding of the nutrient
# totals moves the density by more than the tolerance. The tolerance is 1% of the item's factor, but never less than
# 0.5 kcal or 0.05 g of protein per 100 g, for items with very small factors (such as tea).
NUTRIENT_FACTOR_MIN_FOOD_TONNES = 1000
NUTRIENT_FACTOR_RELATIVE_TOLERANCE = 0.01
NUTRIENT_FACTOR_ABSOLUTE_TOLERANCE = {"energy": 0.5, "protein": 0.05}

# Assumption 6, minor: an item in the fixed-density list keeps its data-derived density if more than this share of
# its supply is eaten as food.
FIXED_DENSITY_MAX_FOOD_SHARE = 0.01
# Assumption 13, minor: a region-year is dropped when the member countries with a balance hold less than this share
# of the region's population.
MIN_FRACTION_POPULATION_COVERED = 0.8
# Assumption 12, major for the region aggregates: the OWID regions rebuilt in this step; the FAOSTAT garden step's
# rows for them, if any, are dropped.
REGIONS = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    # "Low-income countries" is left out (assumption 13): SCL covers at most 80% of its population.
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
]
# Columns of the per-item balance table shared by the SCL and the FBS (fish) parts.
BALANCE_COLUMNS = (
    ["country", "year", "item_code", "fao_item", "role"]
    + BALANCE_ELEMENTS
    + ["food_kcal_per_year", "food_protein_g_per_year", "food_tonnes_for_density"]
)


# --------------------------------------------------------------------------------------------------------------------
# Helpers.
# --------------------------------------------------------------------------------------------------------------------
def _pad_code(code: int) -> str:
    # Item codes in the FAOSTAT garden tables are zero-padded to 8 characters ("00002511").
    return str(code).zfill(8)


# --------------------------------------------------------------------------------------------------------------------
# The items file (assumptions 6 to 9).
# --------------------------------------------------------------------------------------------------------------------
def load_manual_inputs() -> dict:
    """Load the items file: fixed densities, never-food items, implied-density families, overrides, fish, checks.

    Implements assumptions 6 and 7 (their manual inputs), plus the role override of assumption 8 and the fish list
    of assumption 9.
    """
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


# --------------------------------------------------------------------------------------------------------------------
# Roles (assumption 8).
# --------------------------------------------------------------------------------------------------------------------
def load_roles(tb_groups: Table) -> pd.Series:
    """Map each SCL item code to its role, from FAO's item groups.

    Implements assumption 8 (the roles).
    """
    groups = tb_groups[tb_groups["item_group"].astype(str) != "Grand Total"]
    groups = groups[["item_code", "item", "item_group"]].copy()
    groups["item_code"] = groups["item_code"].astype(int).map(_pad_code)
    groups["item_group"] = groups["item_group"].astype(str)
    assert not groups["item_code"].duplicated().any(), "An SCL item belongs to more than one FAO item group."
    unknown = set(groups["item_group"]) - set(ITEM_GROUP_ROLES)
    assert not unknown, f"Unknown FAO item groups in SCL: {unknown}"
    return groups.set_index("item_code")["item_group"].map(ITEM_GROUP_ROLES)


def sanity_check_inputs(tb: Table, roles: pd.Series, manual: dict) -> None:
    """Check the element units of assumption 3, the role coverage of assumption 8, and the items file names."""
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


# --------------------------------------------------------------------------------------------------------------------
# The balance table (assumptions 1 and 2).
# --------------------------------------------------------------------------------------------------------------------
def prepare_balance_table(tb: Table, roles: pd.Series, manual: dict) -> Table:
    """One row per (country, year, item) with one column per element, for all SCL items.

    Implements assumption 2 (missing elements are treated as zero); the rest is reshaping, with no data decision.
    """
    tb = tb[tb["element_code"].isin(ELEMENTS) & (tb["item_code"].astype(str) != POPULATION_ITEM_CODE)].reset_index(
        drop=True
    )
    tb = tb[["country", "year", "item_code", "fao_item", "element_code", "value", "population_with_data"]].astype(
        {"country": str, "item_code": str, "fao_item": str, "value": float, "population_with_data": float}
    )
    # Countries only: the FAOSTAT garden step's region rows, if any, are dropped and rebuilt in `add_region_aggregates`.
    tb = tb[~tb["country"].isin(REGIONS)].reset_index(drop=True)
    # OWID population of the country, which the FAOSTAT garden step attaches to every row.
    population = tb.groupby(["country", "year"])["population_with_data"].agg(["min", "max"])
    assert (population["min"] == population["max"]).all(), "Population differs across rows of a country-year."
    population = population["max"].rename("population").reset_index()
    names = tb[["item_code", "fao_item"]].drop_duplicates().set_index("item_code")["fao_item"]
    tb = tb.drop(columns=["fao_item", "population_with_data"]).pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in ELEMENTS.items()})
    for element in ELEMENTS.values():
        if element not in tb.columns:
            tb[element] = np.nan
    tb["food_kcal_per_year"] = tb["food_kcal_per_year"] * KCAL_PER_MILLION_KCAL
    # FAO reports the food protein in tonnes; grams are the density's own unit.
    tb["food_protein_g_per_year"] = tb.pop("food_protein_tonnes_per_year") * GRAMS_PER_TONNE
    tb[BALANCE_ELEMENTS] = tb[BALANCE_ELEMENTS].fillna(0)
    tb["fao_item"] = tb["item_code"].map(names)
    tb["role"] = tb["item_code"].map(roles)
    for override in manual["role_overrides"]:
        assert override["role"] in ROLES, f"Unknown role in override: {override}"
        tb.loc[tb["item_code"] == _pad_code(override["code"]), "role"] = override["role"]
    assert tb["role"].notnull().all()
    # "food_tonnes_for_density" is the denominator of the density. For SCL items it is simply the balance element
    # "food" (SCL reports exact tonnes, so there is no rounding to avoid); the fish rows taken from FBS get a
    # per-capita-derived value instead, in `prepare_fish_table`.
    tb["food_tonnes_for_density"] = tb["food"]
    tb = tb.merge(population, on=["country", "year"], how="left")
    assert tb["population"].notnull().all(), "Some SCL rows have no population."
    return tb[BALANCE_COLUMNS + ["population"]]


# --------------------------------------------------------------------------------------------------------------------
# Fish from FBS (assumption 9).
# --------------------------------------------------------------------------------------------------------------------
def prepare_fish_table(tb_fbsc: Table, manual: dict, population: Table) -> Table:
    """FBS fish items, reshaped like the SCL balance table.

    Implements assumption 9.
    """
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
    # Only the countries and years SCL covers (the population table comes from the SCL rows). FBS tonnages are
    # rounded to 1,000 t, so densities are taken from its per-capita figures, expressed as totals in SCL's units
    # (kcal per year; tonnes of protein per year; tonnes of food) so that they can be summed into regions.
    tb = tb.merge(population, on=["country", "year"], how="inner")
    # Multiplying by population would copy population's display settings (rounding, projection flag) into every
    # stage, through the densities.
    tb["population"].metadata.display = None
    tb["food_kcal_per_year"] = tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    tb["food_protein_g_per_year"] = tb["food_protein_g_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    tb["food_tonnes_for_density"] = tb["food_kg_per_capita_per_year"] * tb["population"] / KG_PER_TONNE
    return tb[BALANCE_COLUMNS + ["population"]]


# --------------------------------------------------------------------------------------------------------------------
# Regions (assumptions 12 and 13).
# --------------------------------------------------------------------------------------------------------------------
def add_region_aggregates(tb: Table) -> Table:
    """Build the OWID region aggregates and drop the region-years with low population coverage.

    Implements assumptions 12 and 13.
    """
    assert not tb["country"].isin(REGIONS).any(), "Region rows must be dropped before aggregating."
    keys = ["country", "year", "item_code"]
    value_columns = [c for c in tb.columns if c not in keys + ["population"] and pd.api.types.is_numeric_dtype(tb[c])]
    item_columns = [c for c in tb.columns if c not in keys + ["population"] + value_columns]
    item_attributes = tb[["item_code"] + item_columns].drop_duplicates()
    assert not item_attributes["item_code"].duplicated().any(), "Item attributes differ across rows of an item."
    population = tb[["country", "year", "population"]].drop_duplicates()
    assert not population.duplicated(["country", "year"]).any(), "Population differs across items of a country-year."

    # A total that no member reports (FAO gives no food nutrients for cakes, for one) stays missing, not zero.
    flows = paths.regions.add_aggregates(
        tb[keys + value_columns],
        regions=REGIONS,
        index_columns=keys,
        aggregations={c: "sum" for c in value_columns},
        min_num_values_per_year=1,
    )
    # Assumption 13 rides on this call: min_frac_population gates the summed population itself, so a region-year
    # whose members with a balance hold less than MIN_FRACTION_POPULATION_COVERED of the region's population (taken
    # from the population dataset) comes back with a nan population.
    population = paths.regions.add_aggregates(
        population,
        regions=REGIONS,
        index_columns=["country", "year"],
        aggregations={"population": "sum"},
        min_frac_population=MIN_FRACTION_POPULATION_COVERED,
    )
    tb = flows.merge(item_attributes, on="item_code", how="left").merge(population, on=["country", "year"], how="left")

    # Drop the gated region-years, so that a value labeled "Africa" is never built from a small fraction of Africa.
    dropped = tb.loc[tb["population"].isnull(), ["country", "year"]].drop_duplicates()
    assert set(dropped["country"]) <= set(REGIONS), "A country lost its population in the region aggregation."
    assert set(dropped["country"]) == REGIONS_WITH_LOW_COVERAGE, (
        f"Regions dropped for low coverage changed: {sorted(set(dropped['country']))}. Update the docstring."
    )
    return tb[tb["population"].notnull()].reset_index(drop=True)


def sanity_check_nutrient_factors(tb: Table) -> None:
    """Check that FAO uses one calorie factor and one protein factor per item, except for the listed countries.

    Checks the fact stated in assumption 3, on countries only (before fish and regions are added).
    """
    # The European Union is kept as an entity, but it is an aggregate whose density mixes its members' factors.
    tb = tb[(tb["food"] >= NUTRIENT_FACTOR_MIN_FOOD_TONNES) & (tb["country"] != "European Union (27)")]
    deviating = set()
    for nutrient in ["energy", "protein"]:
        density = tb[NUTRIENTS[nutrient]["numerator"]] / (tb["food"] * HUNDRED_GRAMS_PER_TONNE)
        rows = tb[density.notnull()]
        density = density[density.notnull()]
        # The factor of an item is its most common density across all countries and years.
        factor = density.groupby(rows["fao_item"]).transform(lambda d: d.round(2).mode().iloc[0])
        tolerance = np.maximum(
            NUTRIENT_FACTOR_RELATIVE_TOLERANCE * factor, NUTRIENT_FACTOR_ABSOLUTE_TOLERANCE[nutrient]
        )
        off = (density - factor).abs() > tolerance
        deviating |= set(zip(rows.loc[off, "country"], rows.loc[off, "fao_item"]))
    assert deviating == COUNTRY_SPECIFIC_NUTRIENT_FACTORS, (
        f"Country-specific FAO nutrient factors changed. New: {sorted(deviating - COUNTRY_SPECIFIC_NUTRIENT_FACTORS)}. "
        f"Gone: {sorted(COUNTRY_SPECIFIC_NUTRIENT_FACTORS - deviating)}."
    )


def sanity_check_balance_identity(tb: Table) -> None:
    """Check assumptions 1 and 2: SCL balances close in tonnes."""
    supply = tb["production"] + tb["imports"] - tb["exports"] - tb["stock_variation"]
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * uses.abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_open = ((supply - uses).abs() > tolerance).mean()
    assert share_open < 0.01, f"Supply differs from the sum of uses in {100 * share_open:.2f}% of item balances."


# --------------------------------------------------------------------------------------------------------------------
# Densities (assumptions 3 to 7).
# --------------------------------------------------------------------------------------------------------------------
def fixed_density_map(tb: Table, manual: dict, nutrient: str) -> dict[str, float]:
    """Fixed densities per item code for one nutrient, from the families (by name pattern) and the explicit items.

    Implements assumption 6.
    """
    fixed = {}
    for family in manual["fixed_densities"]["families"]:
        for code, name in tb[["item_code", "fao_item"]].drop_duplicates().itertuples(index=False):
            if name.startswith(family["pattern"]):
                fixed[code] = family[nutrient]
    for item in manual["fixed_densities"]["items"]:
        fixed[_pad_code(item["code"])] = item[nutrient]
    return fixed


def add_densities(tb: Table, manual: dict, nutrient: str) -> Table:
    """Densities per (country, year, item) for one nutrient.

    Implements assumptions 3 (derivation), 4 (fallbacks), 5 (zero treatment), 6 (fixed) and 7 (implied by
    products).
    """
    assert nutrient != "mass", "The mass table involves no density; it never goes through this function."
    tb = tb.copy()
    # The nutrient decides the ingredients (see NUTRIENTS): which food total is the numerator of the density, and
    # the physical ceiling.
    numerator = NUTRIENTS[nutrient]["numerator"]
    ceiling = NUTRIENTS[nutrient]["ceiling"]

    # Assumption 3: the density is the food nutrient eaten per year (kilocalories, or grams of protein) divided by
    # the number of 100 g portions of food eaten per year.
    raw = tb[numerator] / (tb["food_tonnes_for_density"] * HUNDRED_GRAMS_PER_TONNE)
    # The pathologies of that division count as missing: division by zero tonnes (infinity), zero by zero (nan),
    # and negative values.
    raw = raw.where(np.isfinite(raw) & (raw >= 0))
    # Assumption 4: a density above the physical ceiling is a broken FAOSTAT cell, and counts as missing too.
    within_ceiling = raw.where(raw <= ceiling)
    # A density of exactly zero is real for oils and sugars (no protein), but a small nutrient amount can also
    # be rounded to zero, so zeros are not used directly: they enter the medians, which come out as zero
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

    # Assumption 6: items nobody eats, valued as human food; items that are never food, zero.
    fixed = fixed_density_map(tb, manual, nutrient)
    for item in manual["never_food"]:
        fixed[_pad_code(item["code"])] = 0.0
    # Fixed densities take precedence over the data for items that are not really food: a few hundred tonnes of soybean
    # cake eaten somewhere would otherwise set the density of hundreds of millions of tonnes. Items in the list with
    # a real food use (spirits, wheat bran) keep their data-based density.
    supply = (tb["production"] + tb["imports"]).abs().groupby(tb["item_code"]).transform("sum")
    food_share = tb["food"].abs().groupby(tb["item_code"]).transform("sum") / supply
    is_fixed = tb["item_code"].isin(fixed) & (food_share.fillna(0) < FIXED_DENSITY_MAX_FOOD_SHARE)
    tb.loc[is_fixed, "density"] = tb.loc[is_fixed, "item_code"].map(fixed)
    tb.loc[is_fixed, "density_source"] = "fixed"
    never = tb["item_code"].isin([_pad_code(item["code"]) for item in manual["never_food"]])
    tb.loc[never, ["density", "density_source"]] = [0.0, "never_food"]

    # Assumption 7: crops not eaten as harvested get the density implied by their products, from World each year.
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
        assert implied.between(0, ceiling).all(), error
        mask = tb["item_code"].isin(crops)
        tb.loc[mask, "density"] = tb.loc[mask, "year"].map(implied)
        tb.loc[mask, "density_source"] = "implied_by_products"

    unresolved = tb[tb["density"].isnull() & (tb[BALANCE_ELEMENTS].abs().sum(axis=1) > 0)]
    error = f"Items with nonzero elements but no {nutrient} density (add them to the items file): " + str(
        unresolved.groupby("fao_item")["production"].sum().sort_values(ascending=False).round(0).to_dict()
    )
    assert unresolved.empty, error
    tb["density"] = tb["density"].fillna(0.0)
    return tb


def sanity_check_densities(tb: Table, nutrient: str) -> None:
    """Check the scales promised in assumption 4: every tonne has a density, and most tonnage uses a direct one."""
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."
    mass = (tb["production"] + tb["imports"]).abs()
    provenance = mass.groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    # In SCL, staples such as paddy rice and sugar cane are only eaten after processing, so a large share of supply
    # legitimately uses median or product-implied densities rather than a direct one.
    assert provenance.get("none", 0) == 0, "Some supply has no density."
    assert provenance.get("direct", 0) > 55, (
        f"Only {provenance.get('direct', 0):.1f}% of supply uses a direct {nutrient} density."
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


# --------------------------------------------------------------------------------------------------------------------
# The chain (assumptions 3, 8, 10, 11 and 14).
# --------------------------------------------------------------------------------------------------------------------
def convert_elements_to_nutrient(tb: Table, nutrient: str) -> Table:
    """Convert elements from tonnes, retaining reported food nutrients where the direct density is accepted.

    Implements the conversion half of assumption 3.
    """
    converted = tb[["country", "year", "role"]].copy()
    if nutrient == "mass":
        # The mass table involves no density: tonnes are converted to kilograms, nothing else.
        for element in BALANCE_ELEMENTS:
            converted[element] = tb[element] * KG_PER_TONNE
    else:
        for element in BALANCE_ELEMENTS:
            converted[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["density"]
        # Fish from FBS has coarser food tonnage than the figures used to derive its density.
        # Keep the reported nutrient total only where that density survives all checks and overrides.
        direct = tb["density_source"] == "direct"
        converted.loc[direct, "food"] = tb.loc[direct, NUTRIENTS[nutrient]["numerator"]]
    return converted


def sum_items_into_stages(converted: Table, population: Table) -> Table:
    """Split the production of each item by its role, and sum all items into the stages of the chain.

    Implements assumption 8. The output has one row per country and year, with one column per stage.
    """
    converted["crop_production"] = converted["production"].where(converted["role"] == "crop", 0)
    converted["animal_products"] = converted["production"].where(converted["role"] == "animal", 0)
    converted["processed_production"] = converted["production"].where(converted["role"] == "processed", 0)
    converted = converted.drop(columns=["production", "role"])
    chain = converted.groupby(["country", "year"], as_index=False).sum(min_count=1)
    chain = chain.merge(population, on=["country", "year"], how="left")
    chain["processing_net"] = chain["processing"] - chain["processed_production"]
    chain = chain.drop(columns=["processing", "processed_production"])
    chain = chain.rename(columns={"residuals": "data_adjustments"})
    return chain


def move_rounding_gap_to_adjustments(chain: Table) -> Table:
    """Move the gap between converted stages and food to "data_adjustments", so that the chain ends on "food".

    Implements assumption 10. The size of the gap is kept in "balancing_difference" for quality control.
    """
    chain_end = chain["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - chain[stage] if stage in SUBTRACTED_STAGES else chain_end + chain[stage]
    chain["balancing_difference"] = chain["food"] - chain_end
    chain["data_adjustments"] = chain["data_adjustments"] - chain["balancing_difference"]
    return chain


def equalize_world_trade(chain: Table) -> Table:
    """Set World exports equal to World imports; the difference goes to "data_adjustments".

    Implements assumption 11.
    """
    world = chain["country"] == "World"
    trade_gap = chain.loc[world, "imports"] - chain.loc[world, "exports"]
    assert (trade_gap.abs() < MAX_WORLD_TRADE_GAP * chain.loc[world, "imports"]).all(), (
        f"World imports and exports differ by up to {100 * (trade_gap / chain.loc[world, 'imports']).abs().max():.1f}%."
    )
    chain.loc[world, "exports"] = chain.loc[world, "imports"]
    chain.loc[world, "data_adjustments"] = chain.loc[world, "data_adjustments"] - trade_gap
    return chain


def remove_world_tourist_consumption(chain: Table) -> Table:
    """Set World tourist consumption to zero; the removed amount goes to "data_adjustments".

    Implements assumption 11.
    """
    world = chain["country"] == "World"
    tourist = chain.loc[world, "tourist_consumption"]
    assert (tourist.abs() < MAX_WORLD_TOURIST_CONSUMPTION * chain.loc[world, "food"]).all(), (
        f"World tourist consumption is up to {100 * (tourist / chain.loc[world, 'food']).abs().max():.2f}% of food."
    )
    chain.loc[world, "tourist_consumption"] = 0
    chain.loc[world, "data_adjustments"] = chain.loc[world, "data_adjustments"] + tourist
    return chain


def per_person_per_day(chain: Table) -> Table:
    """Divide every stage by the entity's population and by 365 days.

    Implements assumption 14. For regions, the population is that of the summed member countries (assumption 12).
    """
    assert chain["population"].notnull().all() and (chain["population"] > 0).all(), "Missing population."
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    return chain[["country", "year"] + STAGES]


# --------------------------------------------------------------------------------------------------------------------
# Output checks.
# --------------------------------------------------------------------------------------------------------------------
def sanity_check_outputs(tb: Table, tb_fbsc: Table, nutrient: str) -> None:
    """Check one output table: shape, no negative magnitudes, the chain ends on food, and World matches FAO."""
    assert tb.columns[tb.isna().all()].empty, "Output has fully-nan columns."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    for stage in [
        s for s in STAGES if s not in ["stock_variation", "data_adjustments", "processing_net", "balancing_difference"]
    ]:
        # FAO occasionally reports a negative element (Iraq 2010 wheat exports, for one); small negatives are tolerated.
        assert (tb[stage].fillna(0) >= -0.01 * tb["food"].abs()).all(), (
            f"Negative values in stage {stage!r} ({nutrient})."
        )
    chain_end = tb["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - tb[stage] if stage in SUBTRACTED_STAGES else chain_end + tb[stage]
    assert (chain_end - tb["food"]).abs().max() < 1e-3 * tb["food"].abs().max(), "Chain does not land on food."
    world_trade = tb.loc[tb["country"] == "World", ["imports", "exports"]]
    assert (world_trade["imports"] == world_trade["exports"]).all(), f"World imports and exports differ ({nutrient})."

    world = tb[tb["country"] == "World"].set_index("year")
    # Our food stage for World against FAO's own published World (the FBS "Grand Total" item in `faostat_fbsc`; FAO's
    # World row is harmonized to "World", and the upstream pipeline builds no World of its own). Our World is rebuilt
    # from countries, so this compares our construction against the producer's.
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
    assert deviation.abs().max() < 0.05, (
        f"World food {nutrient} deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    )
    world_processing_share = (world["processing_net"] / world["crop_production"]).abs().max()
    assert world_processing_share < 0.1, (
        f"Net processing is {100 * world_processing_share:.0f}% of crop production for World ({nutrient})."
    )


# --------------------------------------------------------------------------------------------------------------------
# Main.
# --------------------------------------------------------------------------------------------------------------------
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
    # Assumption 12: FAO's own regional aggregates are dropped (OWID regions are built later from countries).
    tb_scl = tb_scl[~tb_scl["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    tb_fbsc = tb_fbsc[~tb_fbsc["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    # Assumption 8: the role of every item, from FAO's item groups.
    roles = load_roles(tb_groups)
    sanity_check_inputs(tb_scl, roles=roles, manual=manual)

    # Assumptions 1 and 2: the balance table, with missing elements as zero.
    tb = prepare_balance_table(tb_scl, roles=roles, manual=manual)
    sanity_check_nutrient_factors(tb)
    population = tb[["country", "year", "population"]].drop_duplicates()
    # Assumption 9: fish and seafood, taken from FBS.
    tb_fish = prepare_fish_table(tb_fbsc, manual=manual, population=population)
    tb = pr.concat([tb, tb_fish], ignore_index=True)
    # Assumptions 12 and 13: OWID regions from member countries, dropping region-years with low coverage.
    tb = add_region_aggregates(tb)
    sanity_check_balance_identity(tb)

    population_by_entity = tb[["country", "year", "population"]].drop_duplicates()
    tables = []
    for nutrient in NUTRIENTS:
        if nutrient == "mass":
            # The mass table involves no density: tonnes are converted to kilograms, nothing else.
            converted = convert_elements_to_nutrient(tb, nutrient=nutrient)
        else:
            # Assumptions 3 to 7: the density of every item, with its fallbacks, zero treatment, fixed and implied
            # values.
            tb_nutrient = add_densities(tb, manual=manual, nutrient=nutrient)
            sanity_check_densities(tb_nutrient, nutrient=nutrient)
            sanity_check_processing_families(tb_nutrient, manual=manual, nutrient=nutrient)
            # Assumption 3: convert tonnes, retaining reported food nutrients where the direct density is accepted.
            converted = convert_elements_to_nutrient(tb_nutrient, nutrient=nutrient)
        # Assumption 8: production split by role, items summed into the stages of the chain.
        chain = sum_items_into_stages(converted, population=population_by_entity)
        # Assumption 10: the rounding gap goes to data adjustments, so the chain ends exactly on food.
        chain = move_rounding_gap_to_adjustments(chain)
        # Assumption 11: World exports are set equal to World imports, and World tourist consumption is set to zero.
        chain = equalize_world_trade(chain)
        chain = remove_world_tourist_consumption(chain)
        # Assumption 14: per person per day.
        chain = per_person_per_day(chain)
        sanity_check_outputs(chain, tb_fbsc=tb_fbsc, nutrient=nutrient)
        tables.append(chain.format(["country", "year"], short_name=nutrient))

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
