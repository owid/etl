"""FAOSTAT Food Balance Sheets (FBS) as a chain of stages from crop production to food, per person per day.

FBS elements in tonnes are converted with per-item densities derived from FBS itself, and summed into the
stages of a chain from crop production to food available to eat. Food keeps its reported calories and protein
where the directly derived density is accepted; otherwise it uses the replacement density. The chain is built
three times, in three units, one table each:
    energy   kilocalories per person per day
    protein  grams of protein per person per day
    mass     kilograms per person per day (the balance in tonnes, with no conversion at all)

ASSUMPTIONS THAT GO INTO THE CALCULATION
-----------------------------------------
1. The balance identity.
   >> Scale: major. The identity is the backbone of every table.

   For every item, country and year, FBS reports, in tonnes:
       production + imports - exports - stock variation
         = food + feed + seed + processing + other uses + losses + tourist consumption + residuals.

2. Derived stock variation.
   >> Scale: minor. Stock variation is a small stage; for World in 2023, 88 kcal per person per day, against a food
   stage of about 3,000.

   FBS only reports stock variation from 2010 onward, so this step derives stock variation
   for all years from the identity, as production + imports - exports - domestic supply. Where FBS reports stock
   variation (2010 onward), a check verifies that the derived value agrees with the reported one.

3. Missing elements are treated as zero.
   >> Scale: minor. Bookkeeping that changes no values; it only lets the identity be evaluated for every item.

   When FBS does not report an element for an item (meat has no "seed", for
   example), the step treats the missing element as zero, so that the identity can be evaluated for every item.
   This is safe for two reasons. First, FAO builds each balance as a whole, estimating every element of the item
   together, so an element missing from a compiled balance is almost always one that does not apply to the item,
   not lost data. Second, if an element with a real value were missing and treated as zero, the two sides of the
   identity would not close for that item, and the step fails when more than 2% of the item balances do not close.

4. Densities.
   >> Scale: major. These densities convert all stages other than directly reported food into calories and protein.

   FBS reports every element of the balance only in tonnes. The one exception is food: for each item,
   country and year, FBS also reports the amount of the item that people eat as a nutrient (kilocalories per person
   per day, or grams of protein per person per day) next to its quantity (kilograms per person per year). To build
   the chain in calories and protein, every other element must be converted from tonnes, and the food element is
   the only place the conversion factor can come from. The density of an item (kcal, or grams of protein, per
   100 g) is the food nutrient per year divided by the food quantity per year. That density,
   derived from the food element alone, is then used to convert the other elements of the item into the nutrient:
   production, imports, exports, stock variation, seed, losses, other uses, processing, feed, tourist consumption
   and residuals. Where this directly derived density is accepted, food keeps its reported nutrient total rather
   than being recalculated from the rounded food tonnage. This preserves food nutrients even when a small food
   quantity is rounded to zero tonnes. Where the density is replaced under assumptions 5 or 6, food continues to
   use the replacement density and the rounded tonnage. Any gap between the converted stages and food goes to
   data adjustments (assumption 9).
   Besides calories and protein, the chain is also built in a third unit, mass. The mass table involves no
   density calculation:
   FBS already reports every element as a mass, and the step only changes the unit from tonnes to kilograms.

5. Rejected and missing densities, and their fallbacks.
   >> Scale: minor, and bounded by a check. Densities derived directly from the data cover more than 90% of tonnage
   (97-99% for World), and an assert fails the step below 90%. The medians cover the rest, mostly the crops not
   eaten as harvested (10% of item balances, 2% of tonnage).

   A density is rejected only when it is physically impossible: more energy than pure fat (920 kcal per 100 g), or
   more than 100 g of protein per 100 g. An impossible density means FAO's nutrient and tonnage figures for that
   item disagree. Densities that are unusual but physically possible are kept as they are.

   A density can also be missing entirely, when the item has no food use in that country.

   In both cases the step falls back to the country's median density for the item over all years. If the country has
   no valid density for the item in any year, it falls back to the item's median over all countries and years.
   Every chain item has a valid density in at least one country and year, so this chain of fallbacks always ends with
   a density; an assert fails the step if a FAOSTAT update ever breaks that.

   Example of an impossible density: soybean oil in the United States in 2023.
   - FAO reports the calories Americans get from soybean oil, and the tonnes of it they eat.
   - Dividing one by the other should give the energy density of soybean oil: roughly 880 kcal per 100 g.
   - Instead it gives 1,510, more than pure fat.
   - This shows a disagreement between the reported calories and food quantity; the ratio alone does not tell us
     which figure needs correcting.
   - The ceiling rejects that value, and the United States' median density for soybean oil (841) is used instead.
     This replacement applies to food as well as the other elements, so the reported food calories are not kept
     for this item and year.
   Vegetable oils are the main case of impossible densities, and the United States the most affected country.
   Wherever this happens, our food stage comes out lower than FAO's own published food supply.

   The second fallback (the median over all countries and years) is the normal path for crops that are rarely eaten
   as harvested, such as sugar cane, sugar beet and cottonseed: most countries never eat them raw, so no country
   median exists. It covers about 10% of item balances, though only about 2% of tonnage, and the density comes from
   the few countries that do eat the item raw.

   Region aggregates (World, continents, income groups) are barely affected. A region's density is computed from its
   members' summed food nutrients over their summed food tonnes, and in a whole region there is almost always some
   country eating the item. World gets its densities from the data for 97-99% of its tonnage and never needs the
   median over all countries.

6. Densities of exactly zero.
   >> Scale: minor. The zero treatment touches 2% of protein cells and 0.1% of energy cells.

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

7. Items.
   >> Scale: major. The 95 included items are the whole dataset. The four excluded items carry at most 1.5% of a
   country's food energy, all of it palm fruit and kernels eaten directly (see "Palm kernels" below).

   Every item code in the FBS table must appear in `food_supply_chain_fbs.items.yml`, in exactly one of three
   lists; an assert fails the step if FAO adds, removes or renames an item.

   - "included": the items that make up the chain, each with a role (crop, animal or processed; see assumption 8).
   - "excluded": items deliberately left out of the chain (see below).
   - "groups": FAO's own totals, such as "Grand Total" and "Cereals - Excluding Beer". Each aggregate group
     is the sum of items that are already in the chain, so adding the aggregate groups would count the same food
     twice. Three of the aggregate groups are used to check that our chain items add up to FAO's totals.

   Four items are excluded:
   - "Population": not a food item.
   - "Alcohol, Non-Food": industrial alcohol, never eaten, and FAO reports no food energy for industrial alcohol.
   - "Meat, Aquatic Mammals": negligible production, and FAO reports no food energy for aquatic mammal meat anywhere.
   - "Palm kernels": the oil palm is a tropical tree grown for its fruit. The flesh of the fruit is pressed into
     palm oil, and the seed inside the fruit (the palm kernel) is pressed into palm kernel oil. Almost the whole
     harvest becomes those two oils. In FBS, the whole harvest of the oil palm is recorded under the item "Palm
     kernels" (FAO defines the item "Palm kernels" as the palm fruit plus the palm kernels). In 93% of the
     country-years that grow oil palm, the item "Palm kernels" has no food use, so no density can be derived for
     it. The step therefore excludes the item "Palm kernels", and the oil palm harvest enters the chain through the
     items "Palm Oil" and "Palmkernel Oil", which are given the role "crop" rather than the role "processed", so
     that the calories of the oil palm start the chain in crop production.
     The cost of excluding "Palm kernels": some people do eat palm fruit and palm kernels directly, and that food
     is lost. FAO reports food energy from "Palm kernels" in 19 countries. The largest share is in the Central
     African Republic: 29 kcal per person per day, which is 1.5% of the country's food.

   The excluded items are left out of all three tables, the mass table included.

8. Roles: where each item's production enters the chain.
   >> Scale: major. Together with the densities, the roles are the shape of the chain: they decide which stage every
   tonne of production lands in.

   Every item has a "production" element, but simply adding up the production of all items would count the same
   calories many times: sugar is made from sugar cane, so the production of sugar repeats calories already counted
   in the production of sugar cane. To avoid double counting, each included item has a role, and the role decides
   where the production of the item goes:

   - "crop": a primary crop (wheat, potatoes, sugar cane). The production of crops is the stage "crop_production",
     the start of the chain.
   - "animal": an animal product (meat, milk, eggs). The chain accounts for animals in two stages.
     First, the stage "feed" subtracts the calories of the crops that
     are fed to animals. Then, the stage "animal_products" adds the calories of the meat, milk and eggs that the
     animals produce. The production of animal items is that second stage. The two stages are far from equal, because
     animals burn most of the calories they eat just by living; the gap between "feed" and "animal_products" is the
     cost of producing animal products.
     Note that the stage "feed" undercounts what animals actually eat: FBS only records the feed use of the items in
     the food balance, and grass, pasture and forage crops (hay, silage) are not FBS items, so everything grazing
     animals eat from pasture enters the chain nowhere. As a consequence, the chain can overstate how efficient
     animals are at converting feed into meat, milk and eggs: the calories in "animal_products" can come close to, or
     even exceed, the calories subtracted in "feed", because part of what the animals really ate (the grass) was
     never subtracted. The distortion is largest for protein and for countries with much grazing livestock.
   - "processed": an item made from other items. Sugar is made from sugar cane; vegetable oils are made from
     oilseeds; beer is made from barley; butter and cream are made from milk.
     Take sugar. The calories in sugar were already counted once, when the sugar cane was produced. Counting the
     production of sugar as a new input would count the same calories twice. But the factories that turn cane into
     sugar cannot be ignored either, because they lose calories along the way.
     So the chain handles factories as one net stage, built from two FAOSTAT elements. The element "processing"
     records what is sent into factories (the sugar cane sent to sugar mills, the oilseeds sent to crushers). The
     element "production", for items whose role is "processed", records what comes out of the factories as food (the
     sugar, the oils). The stage "processing_net" is the difference: the "processing" of all items minus the
     "production" of processed items.
     In the normal case, "processing_net" is positive: fewer food calories leave the factories than enter them,
     because factories lose some calories (milling and crushing are not perfect) and because some calories become
     products that are not food, such as ethanol.
     In many countries and years, however, "processing_net" comes out negative, as if factories created calories;
     what causes that is explained under KNOWN LIMITATIONS below.

9. The gap between the preceding stages and food goes to data adjustments.
   >> Scale: below 2% of the food stage for World; rounding can have a larger effect in small countries.

   FAO publishes FBS tonnages rounded, mostly in units of 1,000 tonnes, so the two sides of the balance identity
   do not close exactly. Food nutrients are retained at their reported precision where the direct density is
   accepted; the other stages still use the rounded tonnages. The gap between those stages and food is included
   in "data_adjustments", together with FAO's own "residuals", so that the chain ends exactly on "food".
   Keeping the more precise food total changes this adjustment without changing the other flows. The size of
   the gap is kept in the column "balancing_difference" for quality control.

10. World exports are set equal to World imports, and World tourist consumption is set to zero.
   >> Scale: minor, and only for World: the trade gap is a few percent of imports, and World tourist
   consumption is at most 0.21% of food.

   The world as a whole does not trade with anyone, so World imports and World exports should be equal. In the
   data they differ, because each is the sum of what individual countries report. World exports are set equal to
   World imports, which FAO considers the better-documented side (FAO 2025, Food Balance Sheets and Supply
   Utilization Accounts Resource Handbook, section 6.1), and the difference goes to "data_adjustments". Other
   regions do trade with the rest of the world, so their imports and exports are left as they are.

   World tourist consumption has the same problem as World trade. "tourist_consumption" is food eaten in a
   country by people who live in another country. Every visitor lives in some country, so the world as a whole
   has no visitors, and World tourist consumption should be zero. In the data it is not zero: from 2010 (the
   first year FAO reports tourist consumption) it is 1 to 6 kcal per person per day, which is 0.04% to 0.21% of
   World food. World tourist consumption is set to zero, and the removed amount goes to "data_adjustments".
   Countries and other regions keep their tourist consumption, because for them food eaten by visitors is real.

11. Regions.
   >> Scale: major for the 11 region aggregates, which only exist through this assumption; no country's values
   change.

   FAO publishes its own regional aggregates; this step drops them and builds OWID regions (World,
    continents, income groups) instead, from the member countries that have an FBS balance that year. Every element
    in tonnes, and the food nutrient totals (per-capita food supply times population), are summed over those
    countries, and the region's population is the sum of those same countries' population. A country either has a
    full balance or no balance at all, so the summed elements and the summed population always cover the same
    countries. Countries that FAO has not compiled (Cuba and North Korea in recent years, and small states) are in
    neither.

12. Low-coverage region-years are dropped.
   >> Scale: minor. It removes a few region-years; no country's values change.

   A region-year is dropped when the countries with a balance hold less
    than 80% of the region's population, so that a value labeled "Africa" is never built from a small fraction of
    Africa. This rule removes "Low-income countries" before 2010 and in 2023, and Oceania in 2002-2009 (Papua New
    Guinea is missing in those years).

13. All stages are divided by that population and by 365 days, to give values per person per day.
   >> Scale: minor. A choice of unit, not of substance; it rescales all values equally.

KNOWN LIMITATIONS
-----------------
- Processing can appear to create calories.
  >> Scale: major for countries like Brazil; small for World. Fixed in `food_supply_chain_scl` for sugar crops,
  though SCL has its own processing problems (ethanol), listed in its docstring.

  In many countries and years, "processing_net" comes out negative, as if factories created calories.
  This happens in 37% of country-years in energy. Brazil is the clearest case: about -670 kcal per person per day
  in 2023, driven by sugar and soybean oil.

  The cause is a structural flaw in the densities. Every density in this step is derived from food use: the
  calories people got from eating an item, divided by the tonnes of the item they ate. That ratio measures how
  many calories a human extracts from the item. For a crop that mostly goes to factories, that is the wrong
  measure, because a factory extracts far more than a human.

  Sugar cane in Brazil shows the problem. Nobody in Brazil eats raw sugar cane, so Brazil has no food use of
  "Sugar cane" to derive a density from. As explained in assumption 5, the step then falls back to the median
  density over all countries and years, and that median comes from the few countries where people chew raw cane
  or drink its juice: 30 kcal per 100 g. The value is genuinely low, not an error: a person chewing cane extracts
  only a small share of the calories in the stalk, because most of the stalk is fibre that is spat out or
  discarded.
  In reality, a mill extracts about 120 kg of sugar from each tonne of cane, so cane bought by mills contains at
  least 43 kcal per 100 g. In the model, each tonne of cane entering the mills is counted at the chewing density,
  30 kcal per 100 g (300,000 kcal per tonne), while the sugar coming out is counted at the well-measured density
  of sugar (at least 430,000 kcal per tonne of cane processed). The model therefore understates the calories
  entering the mills, and the understated calories reappear as calories created in processing.

  This flaw distorts the stages "crop_production" and "processing_net" for crops that are mostly processed
  (sugar cane, sugar beet, cottonseed, the oilseeds). Food keeps its reported nutrients where the direct density
  is accepted. Where food uses a replacement density, the effect is small for crops with little direct food use,
  since that density is multiplied only by the tonnes used as food.
  The sibling step `food_supply_chain_scl` avoids the flaw: there, the density of crops like sugar cane is derived
  from the products made out of them, not from food use.

- Oilseed cakes and other feed by-products are not FBS items.
  >> Scale: major for the stages "feed" and "processing_net", above all in protein. Fixed in
  `food_supply_chain_scl`, which has the cakes as items.

  When oilseeds (soybeans, rapeseed, sunflower seeds) are crushed to extract their oil, the crushed solids that
  remain are called cake. Cake is rich in protein, and it is one of the main things the world feeds to its farm
  animals.

  FBS splits the supply of the soybeans item across its uses, and most of those uses are counted correctly: beans
  eaten by humans are under "food", beans fed whole to animals are under "feed", beans planted are under "seed".
  The problem is the beans sent to be crushed, which are counted under "processing". The chain counts all the
  calories and protein contained in those beans as entering the factories (the tonnes of beans multiplied by the
  density of soybeans). But FBS has an item only for one of the two things that come out: the oil. The cake
  is not an FBS item, so the calories and protein of the cake never come out of "processing" and never reach
  "feed", even though in reality the cake is fed to animals.

  As a result, nutrients in oilseed cakes appear under "processing_net" rather than "feed". Grass and other feed
  sources are also absent, so these stages do not account for everything animals eat. The sibling step
  `food_supply_chain_scl` includes oilseed cakes as separate items.
- A group's conversion factor can differ from the nutrient content of the raw crop.
  >> Scale: affects the interpretation of stages converted from tonnes.

  FAO groups wheat grain, flour, bread and pasta under "Wheat and products", expressing their quantities as the
  weight of wheat used to produce them. We derive a conversion factor from the group's food calories or protein
  divided by its food quantity, and apply it to production, trade and other uses. The nutrients describe the
  resulting foods, while the quantities are expressed as wheat equivalents, so this factor can differ from the
  nutrient content of raw wheat. FAO describes this method in Food Balance Sheets: A Handbook, section IV.1:
  https://www.fao.org/4/x9892e/X9892e04.htm

- The input dataset (`faostat_fbsc`) combines two FAO datasets.
  >> Scale: minor. Not a flaw to fix; SCL does not have the issue only because SCL starts in 2010.

  `faostat_fbsc` combines FBSH (FAO's old methodology, 1961-2009) and FBS
  (the new methodology, 2010 onward). At the World level the main stages are continuous across the join, with one
  visible artifact: "tourist_consumption" only exists in the new methodology, so it is zero before 2010. The
  combined dataset also keeps countries that FAO removed from its latest release (Japan and ten others, removed
  "due to an ongoing review" since October 2025), by using the previous release for them.

- FBS "Losses" only cover the supply chain, from the farm to the retail shelf.
  >> Scale: a matter of interpretation, not an error. Not fixed in `food_supply_chain_scl`, which inherits the same
  definition from FAO.

  Food thrown away by households,
  restaurants and retailers is not a loss in FBS; it stays inside "food". The stage "food" is therefore the food
  available to eat, not the food actually eaten.

THE WORLD IN 1968
-----------------
This dataset provides historical data for the World waterfall in 1968. The calculated food supply is about
2,330 kcal per person per day. Recorded feed is about 1,120 kcal per person per day, and animal production is
about 440 kcal per person per day.

The limitations described above also apply to 1968: feed excludes grass and oilseed cakes, and the net processing
stage includes nutrients in cakes that are used as feed.
"""

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

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
    # Food supply per capita (all divided by the same population), used only to derive densities.
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
# FAO's aggregate items ("Grand Total", "Vegetal Products", "Animal Products"), used only to check that the
# included items reproduce FAO's totals (checks assumption 7).
TOTAL_ITEM_CODE = "00002901"
VEGETAL_ITEM_CODE = "00002903"
ANIMAL_ITEM_CODE = "00002941"
PARTITION_TOLERANCE = 0.01
# The three units the chain is built in. `numerator` is the column holding the food nutrient total per year, already
# in the density's own unit (kilocalories, or grams of protein); dividing it by the number of 100 g portions of food
# eaten per year gives the density per 100 g. `ceiling` is the physical maximum density. Mass needs neither.
NUTRIENTS = {
    "energy": {"numerator": "food_kcal_per_year", "ceiling": 920, "unit": "kilocalories per person per day"},
    "protein": {"numerator": "food_protein_g_per_year", "ceiling": 100, "unit": "grams of protein per person per day"},
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
# The three item roles of assumption 8. The production of crops starts the chain as "crop_production", the
# production of animal items is added back as "animal_products", and the production of processed items is netted
# against "processing" to give "processing_net".
ROLES = {"crop", "animal", "processed"}
# Output columns, in chain order, as magnitudes in FAO's sign convention; SUBTRACTED_STAGES subtract along the chain.
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
# Unit conversions. Exact by definition; they change no data beyond the choice of unit (assumption 13).
HUNDRED_GRAMS_PER_TONNE = 10_000
KG_PER_TONNE = 1000
DAYS_PER_YEAR = 365

# Checks only; these thresholds change no data, they only decide when the step crashes.
# Tolerance of the identity check, per item balance: 1% of domestic supply plus FAO's rounding (checks
# assumptions 1 to 3).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# Safety limit for assumption 10: World trade is only equalized while the gap is small (about 5% in practice);
# a gap above this share of imports crashes the step instead.
MAX_WORLD_TRADE_GAP = 0.2
# Safety limit for assumption 10: World tourist consumption is only set to zero while it is small (at most 0.21%
# of food in practice); a larger share of food crashes the step instead.
MAX_WORLD_TOURIST_CONSUMPTION = 0.005
# Expected outcome of the coverage rule below; if a FAOSTAT update changes it, the step crashes so that the
# docstring gets updated (checks assumption 12).
REGIONS_WITH_LOW_COVERAGE = {"Low-income countries", "Oceania"}

# Assumption 12, minor: a region-year is dropped when the member countries with a balance hold less than this share
# of the region's population.
MIN_FRACTION_POPULATION_COVERED = 0.8
# Assumption 11, major for the region aggregates: the OWID regions rebuilt in this step; the FAOSTAT garden step's
# rows for them, if any, are dropped.
REGIONS = [
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "High-income countries",
]


# --------------------------------------------------------------------------------------------------------------------
# The items file (assumption 7).
# --------------------------------------------------------------------------------------------------------------------
def load_items_config() -> tuple[Table, dict[str, str], dict[str, str]]:
    """Load the curated items file: chain items, excluded items and aggregate groups.

    Implements assumption 7.
    """
    with open(paths.side_file("food_supply_chain_fbs.items.yml")) as f:
        config = yaml.safe_load(f)
    assert set(config) == {"included", "excluded", "groups"}, "Unexpected top-level keys in items file."
    # Item codes in the FAOSTAT garden tables are zero-padded to 8 characters ("00002511"); pad every code once here.
    for key in ("included", "excluded", "groups"):
        for item in config[key]:
            item["item_code"] = str(item.pop("code")).zfill(8)

    # One row per included item, indexed by code; a duplicated code crashes here.
    items = Table(pd.DataFrame(config["included"])).set_index("item_code", verify_integrity=True)

    excluded = {item["item_code"]: item["name"] for item in config["excluded"]}
    groups = {item["item_code"]: item["name"] for item in config["groups"]}
    all_codes = list(items.index) + list(excluded) + list(groups)
    assert len(all_codes) == len(set(all_codes)), "An item code appears in more than one list of the items file."

    return items, excluded, groups


def sanity_check_inputs(tb: Table, items: Table, excluded: dict[str, str], groups: dict[str, str]) -> None:
    """Check assumption 7 (every FBS item code is in the items file, with its curated name) and the element units."""
    assert {"name", "role"} <= set(items.columns) <= {"name", "role", "fao_group"}, (
        f"Unexpected keys in included items: {sorted(items.columns)}"
    )
    assert items["name"].notnull().all(), "An included item has no name."
    assert items["role"].isin(ROLES).all(), f"Invalid roles: {sorted(set(items['role']) - ROLES)}"
    if "fao_group" in items.columns:
        assert items["fao_group"].dropna().isin({"vegetal", "animal"}).all(), "Invalid fao_group in an included item."

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


# --------------------------------------------------------------------------------------------------------------------
# The balance table (assumptions 1 to 3).
# --------------------------------------------------------------------------------------------------------------------
def prepare_balance_table(tb: Table, items: Table) -> Table:
    """Reshape the FBS table to one row per (country, year, item) with one column per element, for chain items.

    Implements assumptions 2 (derived stock variation) and 3 (missing elements are treated as zero); the rest is
    reshaping, with no data decision.
    """
    tb = tb[tb["item_code"].isin(items.index) & tb["element_code"].isin(ELEMENTS)].reset_index(drop=True)
    tb = tb[["country", "year", "item_code", "element_code", "value", "population_with_data"]].astype(
        {"country": str, "item_code": str, "value": float, "population_with_data": float}
    )
    # Countries only: the FAOSTAT garden step's region rows, if any, are dropped and rebuilt in `add_region_aggregates`.
    tb = tb[~tb["country"].isin(REGIONS)].reset_index(drop=True)
    # OWID population of the country, which the FAOSTAT garden step attaches to every row.
    population = tb.groupby(["country", "year"])["population_with_data"].agg(["min", "max"])
    assert (population["min"] == population["max"]).all(), "Population differs across rows of a country-year."
    population = population["max"].rename("population").reset_index()
    tb = tb.drop(columns=["population_with_data"]).pivot(
        index=["country", "year", "item_code"], columns="element_code", values="value", join_column_levels_with="_"
    )
    tb = tb.rename(columns={code: name for code, name in ELEMENTS.items()})
    assert set(ELEMENTS.values()) <= set(tb.columns), "Some elements are missing after pivoting."

    # A missing balance element means the element is not part of that item's balance (e.g. no seed for meat); treat
    # it as zero so that the identity can be evaluated. Per-capita food (used only for densities) keeps its nans.
    tonnes_columns = [name for code, name in ELEMENTS.items() if code not in PER_CAPITA_ELEMENTS]
    tb[tonnes_columns] = tb[tonnes_columns].fillna(0)

    # Stock variation from the identity (assumption 2). FAO's sign convention: positive means stocks grew.
    tb["stock_variation"] = tb["production"] + tb["imports"] - tb["exports"] - tb["domestic_supply"]

    tb = tb.merge(items[["role"]].reset_index(), on="item_code", how="left")
    assert tb["role"].notnull().all(), "Some rows have no role (item missing from items file)."
    tb = tb.merge(population, on=["country", "year"], how="left")
    assert tb["population"].notnull().all(), "Some FBS rows have no population."
    # Food nutrient totals (kcal per year, tonnes of protein per year) and food tonnes, from FAO's per-capita food
    # supply, so that they can be summed into regions. The density (assumption 4) is their ratio, so for a country
    # the population cancels.
    tb["food_kcal_per_year"] = tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    tb["food_protein_g_per_year"] = tb["food_protein_g_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    # "food_tonnes_for_density" is a second food tonnage, besides the balance element "food": it is reconstructed
    # from FAO's per-capita figure, and it is used only as the denominator of the density. The density then has a
    # numerator and a denominator that both come from FAO's per-capita food supply figures, which share the same
    # population and the same rounding, instead of mixing two differently rounded sources.
    tb["food_tonnes_for_density"] = tb["food_kg_per_capita_per_year"] * tb["population"] / KG_PER_TONNE
    tb = tb.drop(columns=[ELEMENTS[code] for code in PER_CAPITA_ELEMENTS])
    return tb


# --------------------------------------------------------------------------------------------------------------------
# Regions (assumptions 11 and 12).
# --------------------------------------------------------------------------------------------------------------------
def add_region_aggregates(tb: Table) -> Table:
    """Build the OWID region aggregates and drop the region-years with low population coverage.

    Implements assumptions 11 and 12.
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
    # Assumption 12 rides on this call: min_frac_population gates the summed population itself, so a region-year
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


def sanity_check_balance_identity(tb: Table) -> None:
    """Check assumptions 1 to 3: FBS balances close in tonnes, and derived stock variation matches the reported one."""
    # One row of `tb` is one item balance (one item, one country, one year). A balance "closes" when its domestic
    # supply equals the sum of its eight uses, within the tolerance (1% of domestic supply, plus 2,000 tonnes for
    # FAO's rounding). In practice about 0.5% of balances do not close; above 2% something is structurally wrong.
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * tb["domestic_supply"].abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_of_open_balances = ((tb["domestic_supply"] - uses).abs() > tolerance).mean()
    assert share_of_open_balances < 0.02, (
        f"Domestic supply differs from the sum of uses in {100 * share_of_open_balances:.1f}% of item balances."
    )

    # Where FBS reports stock variation (2010 onward; a reported value of exactly zero cannot be told apart from a
    # missing one and is skipped), the derived stock variation must match the reported one, within the same
    # tolerance. In practice about 0.2% of balances mismatch; above 0.5% something is structurally wrong.
    reported = tb[tb["stock_variation_reported"] != 0]
    is_mismatched = (reported["stock_variation"] - reported["stock_variation_reported"]).abs() > tolerance[
        reported.index
    ]
    assert is_mismatched.mean() < 0.005, (
        f"Derived stock variation differs from the reported one in {100 * is_mismatched.mean():.2f}% of item balances."
    )


# --------------------------------------------------------------------------------------------------------------------
# Densities (assumptions 4 to 6).
# --------------------------------------------------------------------------------------------------------------------
def add_densities(tb: Table, nutrient: str) -> Table:
    """Add `density` (nutrient per 100 g) and `density_source` for each (country, year, item).

    Implements assumptions 4 (derivation), 5 (fallbacks) and 6 (zero treatment).
    """
    assert nutrient != "mass", "The mass table involves no density; it never goes through this function."
    tb = tb.copy()
    # The nutrient decides the ingredients (see NUTRIENTS): which food total is the numerator of the density, and
    # the physical ceiling.
    numerator = NUTRIENTS[nutrient]["numerator"]
    ceiling = NUTRIENTS[nutrient]["ceiling"]

    # Assumption 4: the density is the food nutrient eaten per year (kilocalories, or grams of protein) divided by
    # the number of 100 g portions of food eaten per year.
    raw = tb[numerator] / (tb["food_tonnes_for_density"] * HUNDRED_GRAMS_PER_TONNE)
    # The pathologies of that division count as missing: division by zero tonnes (infinity), zero by zero (nan),
    # and negative values.
    raw = raw.where(np.isfinite(raw) & (raw >= 0))
    # Assumption 5: a density above the physical ceiling is a broken FAOSTAT cell, and counts as missing too.
    within_ceiling = raw.where(raw <= ceiling)
    # A density of exactly zero is real for oils and sugars (no protein), but a small nutrient amount can also
    # be rounded to zero, so zeros are not used directly: they enter the medians, which come out as zero
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


def sanity_check_densities(tb: Table, nutrient: str) -> None:
    """Check the scale promised in assumption 5: densities derived directly from the data cover >90% of tonnage."""
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."

    provenance = tb["domestic_supply"].abs().groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    assert provenance.get("direct", 0) > 90, (
        f"Only {provenance.get('direct', 0):.1f}% of domestic supply uses a direct {nutrient} density."
    )


# --------------------------------------------------------------------------------------------------------------------
# The chain (assumptions 4, 8, 9, 10 and 13).
# --------------------------------------------------------------------------------------------------------------------
def convert_elements_to_nutrient(tb: Table, nutrient: str) -> Table:
    """Convert elements from tonnes, retaining reported food nutrients where the direct density is accepted.

    Implements the conversion half of assumption 4.
    """
    converted = tb[["country", "year", "role"]].copy()
    if nutrient == "mass":
        # The mass table involves no density: tonnes are converted to kilograms, nothing else.
        for element in BALANCE_ELEMENTS:
            converted[element] = tb[element] * KG_PER_TONNE
    else:
        for element in BALANCE_ELEMENTS:
            converted[element] = tb[element] * HUNDRED_GRAMS_PER_TONNE * tb["density"]
        # FBS food tonnage is rounded more coarsely than the per-capita food figures used to derive the density.
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
    chain = converted.groupby(["country", "year"], observed=True, as_index=False).sum(min_count=1)
    chain = chain.merge(population, on=["country", "year"], how="left")
    chain["processing_net"] = chain["processing"] - chain["processed_production"]
    chain = chain.drop(columns=["processing", "processed_production"])
    chain = chain.rename(columns={"residuals": "data_adjustments"})
    return chain


def move_rounding_gap_to_adjustments(chain: Table) -> Table:
    """Move the gap between converted stages and food to "data_adjustments", so that the chain ends on "food".

    Implements assumption 9. The size of the gap is kept in "balancing_difference" for quality control.
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

    Implements assumption 10.
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

    Implements assumption 10.
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

    Implements assumption 13. For regions, the population is that of the summed member countries (assumption 11).
    """
    assert chain["population"].notnull().all() and (chain["population"] > 0).all(), "Missing population."
    for stage in STAGES:
        chain[stage] = chain[stage] / chain["population"] / DAYS_PER_YEAR
    return chain[["country", "year"] + STAGES]


# --------------------------------------------------------------------------------------------------------------------
# Output checks.
# --------------------------------------------------------------------------------------------------------------------
def sanity_check_partition(tb_fbsc: Table, items: Table) -> None:
    """Check assumption 7: food supply (kcal) summed over chain items reproduces FAO's own totals, for World."""
    # The comparison needs every item classified as vegetal or animal ("fao_group"). By default the role decides:
    # crops and processed items count as vegetal, animal items as animal. A few items carry an explicit "fao_group"
    # in the items file because FAO classifies them differently from that default: butter, cream and fish oils are
    # processed items that FAO counts as animal, honey is an animal item that FAO counts as vegetal, and seaweed is
    # a crop that FAO counts as animal. The classification is used only by this check.
    fao_group = items["role"].map({"crop": "vegetal", "processed": "vegetal", "animal": "animal"})
    if "fao_group" in items.columns:
        fao_group = items["fao_group"].fillna(fao_group)
    fao_group = fao_group.rename("fao_group")

    world = tb_fbsc[(tb_fbsc["country"] == "World") & (tb_fbsc["element_code"] == "0664pc")]
    world = world[["year", "item_code", "value"]].astype({"item_code": str, "value": float})
    fao = world.pivot(index="year", columns="item_code", values="value")
    curated = world[world["item_code"].isin(items.index)].merge(fao_group.reset_index(), on="item_code")
    ours = curated.pivot_table(index="year", columns="fao_group", values="value", aggfunc="sum")
    ours["total"] = ours["vegetal"] + ours["animal"]
    for group, code in {"total": TOTAL_ITEM_CODE, "vegetal": VEGETAL_ITEM_CODE, "animal": ANIMAL_ITEM_CODE}.items():
        deviation = ((ours[group] - fao[code]) / fao[code]).abs()
        assert deviation.max() < PARTITION_TOLERANCE, (
            f"Chain items do not reproduce FAO's {group} food supply for World; max deviation {100 * deviation.max():.2f}%."
        )


def sanity_check_world_against_fao(tb: Table, tb_fbsc: Table, items: Table) -> None:
    """Check assumption 11: our rebuilt World must match the World that FAO itself publishes.

    We rebuild World by summing countries, instead of using FAO's own World row, because the chain needs its values
    and its population to cover exactly the same countries. This check verifies, element by element and year by
    year, in tonnes, that the rebuilt World stays very close to FAO's published World.
    """
    # FAO's own World, reshaped like our balance table: one row per (year, item), one column per element, in tonnes.
    tonnes_codes = [code for code in ELEMENTS if code not in PER_CAPITA_ELEMENTS]
    fao = tb_fbsc[
        (tb_fbsc["country"].astype(str) == "World")
        & tb_fbsc["item_code"].astype(str).isin(items.index)
        & tb_fbsc["element_code"].astype(str).isin(tonnes_codes)
    ][["year", "item_code", "element_code", "value"]].astype({"item_code": str, "value": float})
    fao = fao.pivot(index=["year", "item_code"], columns="element_code", values="value")
    fao = fao.rename(columns={code: name for code, name in ELEMENTS.items()}).fillna(0)
    fao["stock_variation"] = fao["production"] + fao["imports"] - fao["exports"] - fao["domestic_supply"]
    fao_by_year = fao.groupby("year").sum()
    ours_by_year = tb[tb["country"] == "World"].groupby("year")[BALANCE_ELEMENTS].sum()

    # Stock variation and residuals are small net quantities (additions and subtractions that almost cancel), so a
    # ratio between two small numbers is meaningless; they are checked below by their absolute difference instead.
    for element in [e for e in BALANCE_ELEMENTS if e not in ("stock_variation", "residuals")]:
        # In years where FAO's World reports nothing for the element (tourist consumption before 2010), ours must
        # report next to nothing too; a ratio is only meaningful in the other years.
        fao_is_zero = fao_by_year[element] == 0
        assert (ours_by_year[element][fao_is_zero].abs() < 0.005 * fao_by_year["food"][fao_is_zero]).all(), (
            f"FAO's World reports no {element!r} in some years, but ours does."
        )
        ratio = ours_by_year[element][~fao_is_zero] / fao_by_year[element][~fao_is_zero]
        # Measured 1961-2023: the ratio stays between 0.96 and 1.005. Our World can fall short of FAO's because
        # FAO's World includes countries whose own data FAO does not publish for that year; in 2023 those countries
        # (Japan, Sudan, Cuba and nine others, whose published data end in 2022 or earlier) hold 2.9% of World food.
        assert ratio.between(0.95, 1.01).all(), (
            f"Our World differs from FAO's World for {element!r}: ours/FAO ranges {ratio.min():.3f}-{ratio.max():.3f}."
        )
    for element in ["stock_variation", "residuals"]:
        # Measured 1961-2023: the absolute difference stays below 0.04% of World food.
        difference = (ours_by_year[element] - fao_by_year[element]).abs() / fao_by_year["food"]
        assert (difference < 0.005).all(), (
            f"Our World differs from FAO's World for {element!r} by up to {100 * difference.max():.2f}% of food."
        )


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

    # The chain must end exactly on food once the rounding gap has moved to data adjustments.
    chain_end = tb["crop_production"]
    for stage in STAGES[1:]:
        if stage in ["food", "balancing_difference"]:
            continue
        chain_end = chain_end - tb[stage] if stage in SUBTRACTED_STAGES else chain_end + tb[stage]
    assert (chain_end - tb["food"]).abs().max() < 1e-3 * tb["food"].abs().max(), "Chain does not land on food."
    world_trade = tb.loc[tb["country"] == "World", ["imports", "exports"]]
    assert (world_trade["imports"] == world_trade["exports"]).all(), f"World imports and exports differ ({nutrient})."
    world = tb[tb["country"] == "World"].set_index("year")
    gap = (world["balancing_difference"] / world["food"]).abs()
    assert gap.max() < 0.02, f"FAO rounding gap for World is up to {100 * gap.max():.2f}% of food ({nutrient})."

    # Our food stage for World against FAO's own published World (the "Grand Total" item in `faostat_fbsc`; FAO's
    # World row is harmonized to "World", and the upstream pipeline builds no World of its own). Our World is rebuilt
    # from countries, so this compares our construction against the producer's.
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
    assert deviation.abs().max() < 0.03, (
        f"World food {nutrient} deviates from FAO's total by up to {100 * deviation.abs().max():.1f}%."
    )


# --------------------------------------------------------------------------------------------------------------------
# Main.
# --------------------------------------------------------------------------------------------------------------------
def run() -> None:
    #
    # Load inputs.
    #
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc", safe_types=False)
    # Assumption 7: every FBS item is classified in the items file.
    items, excluded, groups = load_items_config()

    #
    # Process data.
    #
    # Assumption 11: FAO's own regional aggregates are dropped (OWID regions are built later from countries).
    tb_fbsc = tb_fbsc[~tb_fbsc["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    sanity_check_inputs(tb_fbsc, items=items, excluded=excluded, groups=groups)
    sanity_check_partition(tb_fbsc, items=items)

    # Assumptions 1 to 3: the balance table, with derived stock variation and missing elements as zero.
    tb = prepare_balance_table(tb_fbsc, items=items)
    # Assumptions 11 and 12: OWID regions from member countries, dropping region-years with low coverage.
    tb = add_region_aggregates(tb)
    sanity_check_balance_identity(tb)
    sanity_check_world_against_fao(tb, tb_fbsc=tb_fbsc, items=items)

    population = tb[["country", "year", "population"]].drop_duplicates()
    tables = []
    for nutrient in NUTRIENTS:
        if nutrient == "mass":
            # The mass table involves no density: tonnes are converted to kilograms, nothing else.
            converted = convert_elements_to_nutrient(tb, nutrient=nutrient)
        else:
            # Assumptions 4 to 6: the density of every item, with its fallbacks and its zero treatment.
            tb_nutrient = add_densities(tb, nutrient=nutrient)
            sanity_check_densities(tb_nutrient, nutrient=nutrient)
            # Assumption 4: convert tonnes, retaining reported food nutrients where the direct density is accepted.
            converted = convert_elements_to_nutrient(tb_nutrient, nutrient=nutrient)
        # Assumption 8: production split by role, items summed into the stages of the chain.
        chain = sum_items_into_stages(converted, population=population)
        # Assumption 9: the rounding gap goes to data adjustments, so the chain ends exactly on food.
        chain = move_rounding_gap_to_adjustments(chain)
        # Assumption 10: World exports are set equal to World imports, and World tourist consumption is set to zero.
        chain = equalize_world_trade(chain)
        chain = remove_world_tourist_consumption(chain)
        # Assumption 13: per person per day.
        chain = per_person_per_day(chain)
        sanity_check_outputs(chain, tb_fbsc=tb_fbsc, nutrient=nutrient)
        tables.append(chain.format(["country", "year"], short_name=nutrient))

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
