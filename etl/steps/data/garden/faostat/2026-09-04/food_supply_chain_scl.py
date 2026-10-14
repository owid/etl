"""FAO Supply Utilization Accounts (SCL) as a chain of stages from crop production to food, per person per day.

This step builds the same chain as `food_supply_chain_fbs`, but from FAO's Supply Utilization Accounts (SCL)
instead of the Food Balance Sheets (FBS): crop production, plus imports, minus exports, stock changes, seed,
losses, non-food uses, processing and animal feed, plus animal products, ending at the food available to eat.
Food keeps the calories and protein that FAO reports for it, wherever FAO reports them; the other stages are converted
from tonnes. The chain is built three times, in three units, one table each:
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
The price is coverage: SCL starts in 2010 (FBS starts in 1961), misses a few countries that FAO has not
compiled (Japan, Sudan, Somalia and a few others), and has no fish and seafood items at all, which are therefore
taken from FBS (assumption 9).

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
   >> Scale: major. These densities convert every stage except food into calories and protein.

   SCL reports every element of the balance only in tonnes. The exception is food: for each item, country and year,
   SCL also reports the food as calories (the element "Calories/Year") and as protein (the element "Proteins/Year").
   To build the chain in calories and protein, every other element must be converted from tonnes, and the food
   element is the only place the conversion factor can come from. The density of an item (kcal, or grams of
   protein, per 100 g) is the food nutrient divided by the food tonnes. These are FAO's own factors: for all 416
   SCL items with a food use, FAO's food nutrients are its 2024 nutrient conversion table times the item's
   edible-portion coefficient, the share of the item that people eat
   (https://www.fao.org/food-composition/tables-and-databases/detail/global-nutrient-conversion-table-for-fao-supply-utilization-accounts/en).
   FAO uses one factor per item in every country and year (for example 334 kcal per 100 g for wheat grain, 348 for
   maize, 64 for potatoes), except for nine items in a few countries where it uses a factor of its own (potatoes in
   Peru at 91 kcal per 100 g; milled rice in Bangladesh at 360 instead of 349).
   The density converts the item's other elements into the nutrient: production, imports, exports, stock
   variation, seed, losses, other uses, processing, feed, tourist consumption and residuals. Food itself keeps the
   nutrient FAO reports wherever FAO reports one, whatever density the item's other elements end up with under
   assumptions 4 to 7. Keeping it also matters for fish taken from FBS (assumption 9), whose food tonnage is rounded
   more coarsely than the figures used to derive its density. Where FAO reports no food nutrient, food is converted
   like every other element: its food tonnes times the density. Any gap between the converted stages and food goes
   to data adjustments (assumption 10).
   Besides calories and protein, the chain is also built in a third unit, mass. The mass table involves no density
   calculation: SCL already reports every element as a mass, and the step only changes the unit from tonnes to
   kilograms.

4. One factor per country and item, read over all years.
   >> Scale: minor. The country's own factor converts 65% of tonnage (fish by year another 1.5%); the item's
   factor over all countries converts 6.5%. The rest uses the fixed and the product-implied densities of
   assumptions 6 and 7.

   Since FAO's factor for an item in a country is the same every year, the step reads it once: the country's food
   nutrient divided by its food tonnes, both summed over all years. A country that eats less than 1,000 tonnes of
   the item over all years gets the item's factor instead, the same ratio summed over all countries. This matters
   for items that a country produces, trades or feeds to animals but hardly eats, where a few kilograms of food
   would otherwise set the density of millions of tonnes: in 2014 Oceania ate 40 kg of wheat, whose reported
   protein gives 25 g per 100 g against FAO's 11.8, and that density would have converted 26 million tonnes of
   wheat production.
   Years whose food gives no usable ratio are left out of the sums: no food, no reported nutrient, or a ratio above
   the physical maximum (920 kcal per 100 g, the energy of pure fat, or 100 g of protein per 100 g), which only
   happens for sub-tonne quantities, where FAO's rounding breaks the ratio. A reported zero counts as zero, so an
   item with none of the nutrient (sugar and oils have no protein) gets a factor of zero. Regions are treated like
   countries, from the summed food of their members.
   A check fails the step if FAO's factor for an item in a country changes over the years for more than 0.01% of
   food. In 2010 to 2023 only Namibia's skim milk does (from 23 to 36 kcal per 100 g), on 0.0003% of food.

5. Fish keep a density for each year.
   >> Scale: minor. Fish carry 1.5% of tonnage; for World, fish and fish oil are 46 of the 635 kcal per person per
   day of animal products in 2023.

   The FBS fish items are groups, such as pelagic fish or demersal fish, whose species, and so whose density,
   change from year to year in a country (Iceland's pelagic fish runs from 77 to 86 kcal per 100 g). They therefore
   keep the density of each year. A year with no usable food takes the country's factor over all years, whatever
   the tonnage.

6. Items nobody eats get fixed densities.
   >> Scale: major for the stages "feed" and "processing_net". The fixed densities carry only 3.2% of tonnage, but
   that tonnage is the oilseed cakes and brans whose presence is the main reason to use SCL at all.

   Items with little or no food use (cakes, brans, gluten feed, refining residues) need fixed densities. Where FAO's
   2024 nutrient conversion table has the item, its values are used as published: the cereal brans, centrifugal
   cane sugar, hemp oil and tallow. The oilseed cakes and gluten feed are not in that table. Each of them takes the
   composition of one matching table of Feedipedia (the feed tables of INRAE, CIRAD, AFZ and FAO), converted to
   energy with the formula FAO uses for its own table (4 kcal per gram of protein, 9 per gram of fat, 4 per gram of
   available carbohydrate and 2 per gram of fibre), so that each cake is valued on the same basis as the crop and
   the oil it comes from. Soybean cake, for example, gets 307 kcal and 43.4 g of protein per 100 g, from
   Feedipedia's soybean meal of 44% to 46% protein, the type that includes the hulls, as SCL's crush yields imply.
   Five entries have no single published composition and keep the values they had before: bran of pulses,
   hempseed, two fat residues of oil refining, and the default for cakes not listed individually (which only
   reaches "Cake, oilseeds nes"). Chemically modified fats use the factor of FAOSTAT's own food observation. The
   values, their sources and the arithmetic are in `food_supply_chain_scl.items.yml`.
   Listed items always use their fixed densities: a few hundred tonnes of soybean cake eaten somewhere should not
   set the density of hundreds of millions of tonnes of cake. Where at least 1% of an item's supply is eaten as
   food, the step checks that its inferred factor agrees with the fixed density within the rounding tolerance;
   a disagreement stops the step instead of changing the factor.
   Items that are never food in any form are left out of the chain altogether, in all three tables: castor, tung,
   kapok, jojoba, wool grease, cocoa husks, and fuel ethanol (undenatured alcohol of 80% vol or higher). Ethanol is
   the one that matters. The crops sent to distilleries leave the chain where FAO records them: as other uses of
   the crop in Brazil and the United States (2,900 kcal per person per day of cane and molasses in Brazil in 2023),
   or as processing of molasses in Pakistan and India. If ethanol counted as a processed product, those calories
   would be credited back into processing (300 kcal in Brazil, making processing appear to create calories), then
   counted a second time as ethanol exports and ethanol burned, and the fuel that importing countries buy would
   enter their chain as imports (1,500 kcal in the Netherlands). Leaving ethanol out has two costs. Where FAO
   records the distillery feedstock as processing, the calories turned into fuel appear as a processing loss rather
   than under other uses. This is the case in 22 of the 49 countries producing more than 20 kcal per person per
   day of ethanol in 2023 (molasses in Pakistan, India, Mauritius and South Africa; cereals in Belgium, Slovakia,
   Moldova and others), and the amount exceeds 10% of food in Eswatini, Hungary, Paraguay, Slovakia, Tajikistan,
   Belgium and Moldova. And Liberia, where FAO records about 6,300 tonnes a year of
   this ethanol as food, loses 22 kcal of food (1%); it is the only country whose food changes.

7. Crops that go to factories are valued by what the world's factories make of them.
   >> Scale: major for the stages "crop_production" and "processing_net". These values carry 23% of tonnage: sugar
   cane, sugar beet, paddy rice, palm kernels, oil palm fruit, rapeseed, cotton seed, sunflower seed, coconuts,
   olives and maize germ.

   FAO's 2024 nutrient conversion table values each crop at the part of it that people eat, through the crop's
   edible-portion coefficient: sunflower seed without the hull, sugar cane as the juice, coconuts as the meat and
   water of a mix of young and mature nuts, paddy as husked rice, rapeseed as 38% oil and nothing else, cotton seed
   as its kernels. That is the right value for the little that people eat as harvested, but almost the whole
   harvest of these crops goes to factories, which process the whole crop and make recorded products, such as
   cake, bran and molasses, from the parts FAO leaves out. At FAO's value, processing would appear to create
   calories wherever these crops are crushed or milled (sunflower seed in Ukraine, coconuts in the Pacific
   islands), while cotton seed, valued as kernels with no deduction for the hulls, would show large losses.
   These eleven crops are therefore valued by their products: each year, the nutrient in all the products the
   world's factories make from the crop, minus the nutrient of intermediate products processed further within the
   same family (such as copra, pressed into coconut oil), divided by the tonnes of the crop the world processes.
   That World ratio is applied to every country and region. Sugar cane and sugar beet share their products (raw and
   refined sugar, molasses), so they get one value together, 56 kcal per 100 g in 2023. Palm kernels are themselves
   made from oil palm fruit and then crushed for their oil; they are valued first, so that the oil palm fruit
   family counts its kernels at their new value. The families, their products and their order are in the items
   file; they agree with FAO's commodity trees (FAO 2000, Technical Conversion Factors for Agricultural
   Commodities), except that the refined sugar sent to soft drinks and spirits, and the broken rice sent to beer
   and cereal preparations, reach no listed product, which undervalues sugar cane and beet by about 5% and paddy
   by about 1%.

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
   >> Scale: minor for World (fish and fish oil production is 46 of the 635 kcal per person per day of animal
   products in 2023, and 6 of the 43 g of protein); large for fishing nations.

   Fish and seafood are not in SCL. The FBS fish items are added, with their FBS densities under the same rules,
   for the countries and years that SCL covers. FAO has carried most fish rows forward unchanged since 2019 to
   2021 (identical in 2021, 2022 and 2023 for 186 of 198 countries), so the fish figures of recent years are older
   than the rest of the chain.
   Fish oil ("Fish, Body Oil" and "Fish, Liver Oil") has the role "animal", like the fish it comes from, for two
   reasons. First, the FBS fish rows record no processing: the fish ground into meal and oil are inside the fish
   row's production and leave inside its exports and feed, as live weight (Peru's pelagic row exports 5.2 million
   tonnes against 0.5 million eaten: its fishmeal, counted as fish). With the role "processed", the oil would come
   out of processing with no input, and processing would appear to create calories (Iceland, 2,993 kcal per
   person per day). Second, the oil's energy is mostly energy the fish row does not carry: FAO prices fish at the
   energy of their edible flesh, about 79 kcal per 100 g of live anchoveta in Peru, which is about what the meal
   made from 100 g of anchoveta holds (77 kcal), while the oil holds another 40. Counting the oil as an animal
   product adds those calories, and the oil's exports, feed and industrial use then leave the chain like any other
   item's. Imported oil is in no fish row of the importer (Norway imports 987 kcal per person per day of oil
   against 426 of pelagic fish), so it is a real inflow. Protein is unchanged, since the oils have none. The costs
   of this choice are under KNOWN PROBLEMS.

10. The gap between the preceding stages and food goes to data adjustments.
    >> Scale: minor for World (9 kcal per person per day in 2023). Large where people eat coconuts: 14% of food in
    Micronesia in 2023, 7% to 10% in Vanuatu, the Marshall Islands and Kiribati. Rounded fish tonnages can also have
    a larger effect in small countries.

    FAOSTAT rounds tonnages, so balances do not close exactly. Food keeps FAO's reported nutrients at their
    reported precision (assumption 3); the other stages use the reported tonnages. This matters especially for
    fish taken from FBS. The gap between those stages and food is included in "data_adjustments", together with
    FAO's own "residuals" and, in energy and protein, any gain in processing (assumption 15), so that the chain
    ends exactly on "food".
    The gap also holds a difference of valuation, for the crops of assumption 7 that people eat as harvested as
    well as process: mostly coconuts and sugar cane (chewed). The tonnes of these crops that people eat leave the
    earlier stages at the crop's value under assumption 7 (140 kcal per 100 g of coconuts in 2023) but arrive in
    food at FAO's value for the part people eat (85 kcal), and the difference lands in data adjustments. At World
    it is about 9 kcal (coconuts 4.5, sugar cane 3.9); in Micronesia it is 363 kcal per person per day.
    Keeping FAO's food total changes this adjustment without changing the other flows. The size of the gap is kept
    in "balancing_difference" for quality control.

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

    FAO's own regional aggregates are dropped, and OWID regions (World, continents, income groups, the European
    Union) are built from the member countries that have an SCL balance that year: every element and every food nutrient total is summed
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

15. Processing never adds energy or protein: where it would, the gain goes to data adjustments.
    >> Scale: major for a few countries, nil for World. In 2023, 51 of 176 countries would otherwise show
    processing adding energy, 6 of them by more than 10% of food (Guyana 68%, Uruguay 15%, Ukraine 15%, Honduras
    13%, the Netherlands 13%, Papua New Guinea 10%); in protein, 32 countries, 4 above 10% (Guyana, New Zealand,
    Bolivia, Latvia). World shows a loss in both, so it is unchanged.

    A mill, a crusher or a brewery cannot put more energy or protein into its products than came in with the
    inputs. Where the converted products exceed the converted inputs, the excess is a failure of the data or of
    the densities (the causes are under KNOWN PROBLEMS), not a feature of the country's food chain, and a positive
    processing bar would tell readers that mills make food. The gain is therefore moved to "data_adjustments",
    whose meaning is exactly that (assumption 10), and "processing_net" shows zero. Only the gains move: the same
    causes also inflate or shrink the real losses of other countries, and those stay in processing, which remains
    the least certain stage. The rule is applied to each entity and year after the items are summed into stages,
    so a region with a gain (Oceania in every year in energy, and South America, Europe and North America in a few;
    Oceania in 2012 in protein) is treated like a country, and a region's processing is no longer the sum of its
    members'; processing plus data adjustments still is.
    The mass table is left alone: in tonnes a gain in processing can be real, because breweries and bottlers add
    water (in Namibia, 22 thousand tonnes of malt become 174 thousand tonnes of beer), so processing there is the
    net change in weight, in either direction.

KNOWN PROBLEMS, NOT YET RESOLVED
--------------------------------
- Processing would create calories in some countries. The gains are moved to data adjustments; their causes remain.
  >> Scale: major for a few countries: in 2023, the gains moved by assumption 15 exceed 10% of food in six
  countries in energy (Guyana 68%, Uruguay, Ukraine, Honduras, the Netherlands, Papua New Guinea) and four in
  protein (Guyana, New Zealand, Bolivia, Latvia); 33 of 176 countries have a gain above 1% of food in energy and 21
  in protein (23% of all country-years have some gain in energy, 15% in protein). Small for World, where
  processing loses 84 kcal per person per day, 1.4% of crop production. The FBS sibling has the same symptom with
  a different cause.

  In the normal case "processing_net" is positive: the converted processing inputs exceed the converted product
  outputs. The three largest causes are handled: ethanol credited back into processing with no input (Brazil, the
  United States; assumption 6), fish oil (assumption 9), and crops valued at the part that people eat
  (assumption 7; sunflower seed in Ukraine, coconuts in the Marshall Islands and Kiribati). Assumption 15 keeps the
  remaining gains out of the processing bar but not out of the chart: they appear in data adjustments as calories
  entering the chain, more than half of food in Guyana, and the same causes also distort the losses that stay in
  processing elsewhere. The remaining gains have four causes:
  - FAO's own quantities are inconsistent: Guyana's rice bran is 30% of the paddy it mills (Guyana's rice creates
    about 2,250 kcal per person per day in 2023), and in 2022 Bolivia's soybean cake weighs more than the beans
    crushed. One such error is patched in `food_supply_chain_scl.corrections.yml`: since 2021 FAO imputes Denmark's
    molasses production as equal to its whole beet harvest, which made processing create about 2,900 kcal per
    person per day there, and about 40 in the European Union, and inflated feed by the same amounts.
  - Factories that make more from a crop than the world average of assumption 7 (next problem): oil palm in
    Honduras and Papua New Guinea, sunflower seed in Ukraine, sugar beet in Ukraine, the Netherlands and Germany,
    rice in Uruguay.
  - FAO's nutrient factor for a raw item outside assumption 7 is below what its products contain: raw milk against
    its powders and casein (New Zealand in protein, Uruguay in energy).
  - Products with no recorded input: hydrogenated oils, margarine and fat preparations (Netherlands), oilseed
    flours (Bolivia, in protein).
  Recomputing the cake densities from compositions (assumption 6) closed the soybean family, which at the previous
  value of 261 kcal per 100 g of soybean cake lost 9% at World and hid gains of other items: with that loss gone,
  Brazil (sugar cane, beef fats, margarine) and China show energy gains, while the protein gains of Brazil and China
  from soybean cake disappear.

- The values of assumption 7 are World ratios, applied to every country.
  >> Scale: minor for World by construction; major for a few countries whose factories differ from the world
  average.

  A country whose factories make more from a crop than the world average shows a processing gain (Ukraine's
  sunflower mills make 535 kcal per 100 g of seed against the World value of 507, 258 kcal per person per day in
  2023; oil palm in Honduras, 290 against 214), and one whose factories make less shows the difference as a
  processing loss (coconuts in Tonga and Micronesia, 109 and 97 kcal of copra and oil per 100 g of nuts against
  140, about 500 kcal per person per day; oil palm in Malaysia, 205 against 214, 670 kcal). Both mix real
  differences between factories with differences in what FAO records.
  Ukraine's chart is also dominated by FAO's own residuals, which are not ours to fix: 769 kcal per person per day
  in 2023 and about 3,200 in 2020, when FAO records 12.9 million tonnes of maize, sunflower seed and wheat as used
  without a matching supply.

- The stage "feed" only counts feed that passes through the balance.
  >> Scale: "animal_products" exceeds "feed" by more than 1 kcal in 14% of country-years in energy and by more than
  0.01 g in 24% in protein. World still has more recorded feed than animal output (2,051 against 635 kcal in 2023,
  and 103 against 43 g of protein).

  Grass, pasture and forage are not SCL items, so everything grazing animals eat from pasture enters the chain
  nowhere, and wild-caught fish count as animal products with no feed at all. For fishing and grazing countries
  (Iceland, Mongolia, several island states), "animal_products" can therefore exceed "feed". The balance alone
  does not measure how much of a particular country's gap these omitted inputs explain.

- Fish oil is partly counted twice: in calories where the food fish are fatty, in tonnes everywhere.
  >> Scale: about 30% of the world's fish-oil energy, 1.3 of 4.2 kcal per person per day for World in 2018; in
  tonnes, 1.6% to 5% of the animal products of the oil producers. Protein is unaffected.

  The fish row carries about the meal's energy only where the food fish are lean (Peru 79 kcal per 100 g live
  weight, Chile 83, Morocco 76). Where people eat fatty fish, the row's density already exceeds the meal's energy,
  and the oil pressed from the reduced fish is counted a second time: all of Denmark's 285 kcal per person per day
  of oil production in 2018, half of Norway's 279, and nearly all of the small production of Latvia, Japan, Spain,
  Sweden and Ireland. In tonnes the oil is inside the fish tonnes in every producer, so the mass table counts it
  twice: 1.6% of Peru's animal products, 3% of Chile's, 5% of Mauritania's. The alternatives were weighed and
  rejected: leaving the oil out would miss about 70% of its energy (20% of Peru's animal products) and the oil that
  importers buy; counting it in calories but not in tonnes would be exact but is a per-table exception.
  FAO's oil figures exceed what the fish rows can yield in Iceland (13% of pelagic production), Denmark (17%) and
  Mauritania (11%), against about 5% for whole fish; those are data problems, not method problems.
  Tallow and lard are not the same case: SCL records their unrendered-fat input as processing in most country-years,
  so they keep the role "processed"; their remaining processing gains come from FAO's factors (660 kcal per 100 g
  for unrendered fat against 891 for tallow).
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
# FAO uses the same factor for an item in a country every year (checks assumption 4). The check only uses food
# quantities of at least 1,000 tonnes; below that, FAO's rounding of the nutrient totals moves the density by more than
# the tolerance. The tolerance is 1% of the factor, but never less than 0.5 kcal or 0.05 g of protein per 100 g, for
# items with very small factors (such as tea). The food that deviates must stay below this share of all food (in
# 2010 to 2023 it is 0.0003%: Namibia's skim milk, whose factor rises from 23 to 36 kcal per 100 g).
NUTRIENT_FACTOR_MIN_FOOD_TONNES = 1000
NUTRIENT_FACTOR_RELATIVE_TOLERANCE = 0.01
NUTRIENT_FACTOR_ABSOLUTE_TOLERANCE = {"energy": 0.5, "protein": 0.05}
MAX_FOOD_SHARE_OFF_OWN_FACTOR = 0.0001
# Check the fixed factors of assumption 6 against the inferred factor where an item has substantial food use.
# This threshold controls only the check; every listed item uses its fixed factor.
MIN_FOOD_SHARE_FOR_FIXED_DENSITY_CHECK = 0.01

# Assumption 4, minor: an entity's own factor for an item needs at least this much food over all years; below it, the
# item's factor over all countries is used.
MIN_FOOD_TONNES_FOR_OWN_FACTOR = 1000
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
    # Rebuilt from its members like the others, so that it inherits the corrections applied to the country rows;
    # FAOSTAT's own EU aggregate (built upstream, before those corrections) is dropped.
    "European Union (27)",
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
    families = manual["output_implied_densities"]
    for i, family in enumerate(families):
        named.update(family["crops"])
        named.update(family["products"])
        assert set(family["intermediates"]) <= set(family["products"]), "Intermediates must be among the products."
        # A crop that another family makes (palm kernels, made from oil palm fruit) must be valued first, so that the
        # other family counts it at its new value (assumption 7).
        made_earlier = {code for earlier in families[:i] for code in earlier["products"]}
        assert not set(family["crops"]) & made_earlier, f"Move the family of {family['crops']} up in the items file."
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


def drop_never_food_items(tb: Table, manual: dict) -> Table:
    """Leave out the items that are never food, in all three tables.

    Implements the never-food part of assumption 6.
    """
    never = [_pad_code(item["code"]) for item in manual["never_food"]]
    return tb[~tb["item_code"].isin(never)].reset_index(drop=True)


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


def sanity_check_one_factor_per_country(tb: Table) -> None:
    """Check that FAO uses the same calorie and protein factor for an item in a country every year.

    Checks the fact assumption 4 relies on, on countries only (before fish and regions are added).
    """
    tb = tb[tb["food"] >= NUTRIENT_FACTOR_MIN_FOOD_TONNES]
    for nutrient in ["energy", "protein"]:
        numerator = NUTRIENTS[nutrient]["numerator"]
        rows = tb[np.isfinite(tb[numerator])]
        density = rows[numerator] / (rows["food"] * HUNDRED_GRAMS_PER_TONNE)
        pair = [rows["country"], rows["item_code"]]
        factor = rows[numerator].groupby(pair).transform("sum") / (
            rows["food"].groupby(pair).transform("sum") * HUNDRED_GRAMS_PER_TONNE
        )
        tolerance = np.maximum(
            NUTRIENT_FACTOR_RELATIVE_TOLERANCE * factor, NUTRIENT_FACTOR_ABSOLUTE_TOLERANCE[nutrient]
        )
        off = (density - factor).abs() > tolerance
        share = rows.loc[off, "food"].sum() / rows["food"].sum()
        assert share < MAX_FOOD_SHARE_OFF_OWN_FACTOR, (
            f"FAO's {nutrient} factors change over the years for {100 * share:.3f}% of food: "
            f"{sorted(set(zip(rows.loc[off, 'country'], rows.loc[off, 'fao_item'])))[:20]}."
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


def product_implied_density(tb: Table, family: dict, nutrient: str) -> pd.Series:
    """The density of a family's crops each year: the World ratio of the nutrient in its products to the crops processed.

    Implements assumption 7, with the densities already in `tb`. The ratio is summed over countries (World is the sum
    of its countries), so that a family uses the values that earlier families wrote into `tb`. The nutrient of
    intermediates processed further within the family is taken out, since it would otherwise count twice: as their
    own production and as the production of what they become.
    """
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    crops = [_pad_code(c) for c in family["crops"]]
    products = [_pad_code(c) for c in family["products"]]
    intermediates = [_pad_code(c) for c in family["intermediates"]]
    members = tb[~tb["country"].isin(REGIONS) & tb["item_code"].isin(crops + products)]
    made = (members["production"] * members["density"]).where(members["item_code"].isin(products), 0)
    netted = (members["processing"] * members["density"]).where(members["item_code"].isin(intermediates), 0)
    nutrient_out = (made - netted).groupby(members["year"]).sum() * HUNDRED_GRAMS_PER_TONNE
    tonnes_in = members["processing"].where(members["item_code"].isin(crops), 0).groupby(members["year"]).sum()
    years = sorted(set(tb["year"]))
    implied = (nutrient_out / (tonnes_in * HUNDRED_GRAMS_PER_TONNE)).reindex(years)
    assert implied.between(0, ceiling, inclusive="right").all(), (
        f"Implausible implied {nutrient} densities for {list(family['crops'].values())}: {implied.round(1).to_dict()}"
    )
    return implied


def add_densities(tb: Table, manual: dict, nutrient: str) -> Table:
    """Densities per (country, year, item) for one nutrient.

    Implements assumptions 3 (densities from food), 4 (one factor per entity and item), 5 (fish by year), 6 (fixed)
    and 7 (implied by products).
    """
    assert nutrient != "mass", "The mass table involves no density; it never goes through this function."
    tb = tb.copy()
    # The nutrient decides the ingredients (see NUTRIENTS): which food total is the numerator of the density, and
    # the physical ceiling.
    numerator = NUTRIENTS[nutrient]["numerator"]
    ceiling = NUTRIENTS[nutrient]["ceiling"]

    # Assumption 3: the density of a cell is its food nutrient (kilocalories, or grams of protein) over its food, in
    # 100 g portions. Cells with no food, no reported nutrient or a density above the physical ceiling (sub-tonne food
    # quantities, where FAO's rounding breaks the ratio) say nothing about the factor and are left out of the sums.
    cell = tb[numerator] / (tb["food_tonnes_for_density"] * HUNDRED_GRAMS_PER_TONNE)
    valid = np.isfinite(cell) & (cell >= 0) & (cell <= ceiling)
    nutrient_eaten = tb[numerator].where(valid, 0)
    tonnes_eaten = tb["food_tonnes_for_density"].where(valid, 0)
    # Assumption 4: an entity's factor for an item is its food nutrient over its food across all years, if it eats
    # enough of the item; otherwise the item's factor over all countries.
    pair = [tb["country"], tb["item_code"]]
    pair_tonnes = tonnes_eaten.groupby(pair).transform("sum")
    pair_factor = nutrient_eaten.groupby(pair).transform("sum") / (pair_tonnes * HUNDRED_GRAMS_PER_TONNE)
    is_country = ~tb["country"].isin(REGIONS)
    item_tonnes = tonnes_eaten.where(is_country, 0).groupby(tb["item_code"]).transform("sum")
    item_factor = nutrient_eaten.where(is_country, 0).groupby(tb["item_code"]).transform("sum") / (
        item_tonnes * HUNDRED_GRAMS_PER_TONNE
    )
    # Assumption 5: fish keep the density of each year, because the species in an FBS fish group change; a year with
    # no usable food takes the entity's factor over all years, whatever the tonnage.
    is_fish = tb["item_code"].isin([_pad_code(item["code"]) for item in manual["fish_from_fbs"]])
    fish_year = cell.where(is_fish & valid)
    own_factor = pair_factor.where((pair_tonnes >= MIN_FOOD_TONNES_FOR_OWN_FACTOR) | (is_fish & (pair_tonnes > 0)))
    item_factor = item_factor.where(item_tonnes > 0)
    tb["density"] = fish_year.fillna(own_factor).fillna(item_factor)
    tb["density_source"] = np.select(
        [fish_year.notnull(), own_factor.notnull(), item_factor.notnull()],
        ["fish_year", "own_factor", "item_factor"],
        default="none",
    )

    # Assumption 6: listed items always get fixed factors. A few hundred tonnes of soybean cake eaten somewhere
    # should not set the density of hundreds of millions of tonnes. Where food use is substantial, check agreement
    # with the inferred factor before overriding it, using the same rounding tolerance as the other factor check.
    fixed = fixed_density_map(tb, manual, nutrient)
    is_fixed = tb["item_code"].isin(fixed)
    fixed_factor = tb["item_code"].map(fixed)
    supply = (tb["production"] + tb["imports"]).abs().groupby(tb["item_code"]).transform("sum")
    food_share = tb["food"].abs().groupby(tb["item_code"]).transform("sum") / supply
    tolerance = np.maximum(
        fixed_factor.abs() * NUTRIENT_FACTOR_RELATIVE_TOLERANCE, NUTRIENT_FACTOR_ABSOLUTE_TOLERANCE[nutrient]
    )
    disagree = (
        is_fixed
        & (food_share >= MIN_FOOD_SHARE_FOR_FIXED_DENSITY_CHECK)
        & (~np.isfinite(tb["density"]) | ((tb["density"] - fixed_factor).abs() > tolerance))
    )
    assert not disagree.any(), (
        f"Fixed {nutrient} densities disagree with inferred factors for items with substantial food use: "
        + str(
            tb.loc[disagree, ["country", "fao_item", "density"]]
            .assign(fixed_density=fixed_factor[disagree])
            .drop_duplicates()
            .to_dict("records")
        )
    )
    tb.loc[is_fixed, "density"] = fixed_factor[is_fixed]
    tb.loc[is_fixed, "density_source"] = "fixed"

    # Assumption 7: crops that go to factories are valued by what the world's factories make of them, each year.
    # Families are taken in the order of the items file, and each family's values are written into the densities
    # before the next family is read, so that the oil palm fruit family counts its palm kernels at their new value.
    for family in manual["output_implied_densities"]:
        implied = product_implied_density(tb, family=family, nutrient=nutrient)
        mask = tb["item_code"].isin([_pad_code(c) for c in family["crops"]])
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
    """Check the scales promised in assumption 4: every tonne has a density, and most tonnage uses an own factor."""
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."
    mass = (tb["production"] + tb["imports"]).abs()
    provenance = mass.groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    # Crops that go to factories (assumption 7) and items nobody eats (assumption 6) legitimately use other values.
    assert provenance.get("none", 0) == 0, "Some supply has no density."
    own = provenance.get("own_factor", 0) + provenance.get("fish_year", 0)
    assert own > 55, f"Only {own:.1f}% of supply uses the {nutrient} factor of its own country."


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
    """Convert elements from tonnes; food keeps the nutrient FAO reports wherever FAO reports one.

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
        # FAO's food nutrients are its 2024 nutrient conversion table times the edible-portion coefficient, the right
        # value for food whatever density the item's other elements use; tonnes times density only where FAO reports
        # none.
        reported = np.isfinite(tb[NUTRIENTS[nutrient]["numerator"]])
        converted.loc[reported, "food"] = tb.loc[reported, NUTRIENTS[nutrient]["numerator"]]
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


def move_processing_gains_to_adjustments(chain: Table) -> Table:
    """Move any gain in processing to "data_adjustments", so that processing never adds energy or protein.

    Implements assumption 15. Called for energy and protein only: in tonnes, a gain in processing can be real (water
    added in brewing and bottling).
    """
    gain = (-chain["processing_net"]).clip(lower=0)
    chain["processing_net"] = chain["processing_net"] + gain
    chain["data_adjustments"] = chain["data_adjustments"] - gain
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
    # Processing can gain weight (water added in brewing and bottling) but not energy or protein (assumption 15).
    signed = ["stock_variation", "data_adjustments", "balancing_difference"]
    if nutrient == "mass":
        signed.append("processing_net")
    for stage in [s for s in STAGES if s not in signed]:
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
    # Known FAOSTAT errors, patched on the input rows so that the regions built below inherit the fix (see the
    # corrections file for the reasons, and for why this is done here rather than in the garden SCL step).
    tb_scl = paths.apply_corrections(tb_scl)
    # Assumption 8: the role of every item, from FAO's item groups.
    roles = load_roles(tb_groups)
    sanity_check_inputs(tb_scl, roles=roles, manual=manual)

    # Assumptions 1 and 2: the balance table, with missing elements as zero.
    tb = prepare_balance_table(tb_scl, roles=roles, manual=manual)
    # Assumption 6: items that are never food (castor, kapok, fuel ethanol...) are left out of all three tables.
    tb = drop_never_food_items(tb, manual=manual)
    sanity_check_one_factor_per_country(tb)
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
            # Assumptions 3 to 7: the density of every item: FAO's factors read from food, fixed and implied values.
            tb_nutrient = add_densities(tb, manual=manual, nutrient=nutrient)
            sanity_check_densities(tb_nutrient, nutrient=nutrient)
            sanity_check_processing_families(tb_nutrient, manual=manual, nutrient=nutrient)
            # Assumption 3: convert tonnes; food keeps FAO's reported nutrients wherever reported.
            converted = convert_elements_to_nutrient(tb_nutrient, nutrient=nutrient)
        # Assumption 8: production split by role, items summed into the stages of the chain.
        chain = sum_items_into_stages(converted, population=population_by_entity)
        if nutrient != "mass":
            # Assumption 15: processing cannot create energy or protein; any gain goes to data adjustments.
            chain = move_processing_gains_to_adjustments(chain)
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
