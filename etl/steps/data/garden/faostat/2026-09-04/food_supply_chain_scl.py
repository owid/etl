"""FAO Supply Utilization Accounts (SCL) as a chain of stages from crop production to food, per person per day.

WHAT THIS STEP DOES
-------------------
This step follows the calories, protein and weight of each country's food supply, from the crops harvested to the
food available to eat. On the way, food is traded, stored, sown as seed, lost, put to other uses, processed in
factories and fed to animals, which give some of it back as meat, milk and eggs.

The stages of the chain are: crop production, plus imports, minus exports, stock changes, seed, losses, other uses,
processing and animal feed, plus animal products, ending at food.

We build the chain three times, once in each unit, in one table each:
    energy   kilocalories per person per day
    protein  grams of protein per person per day
    mass     kilograms per person per day (the balance in tonnes, with no conversion at all)

Food keeps the calories and protein that FAO reports for it. All other stages are converted from tonnes.

WHY THE SUPPLY UTILIZATION ACCOUNTS
-----------------------------------
FAO publishes two datasets that record these flows: the Food Balance Sheets (FBS), which start in 1961, and the
Supply Utilization Accounts (SCL), which start in 2010. A separate step, `food_supply_chain_fbs`, builds a similar
chain from FBS; nothing in this step depends on it.

We use SCL here because it has two advantages:
a. SCL reports individual products, while FBS reports groups. SCL has "Wheat", "Bread", and other items processed from wheat, as separate items; FBS merges them into one item, "Wheat and products".
b. SCL includes by-products that FBS has no items for: oilseed cakes, brans and gluten feed. When oilseeds such as
   soybeans are crushed for their oil, the solids left over are called cake. Cake is rich in protein, and it is one
   of the main things farm animals eat. With cakes as items, the path from oilseeds to cake to animals shows up under
   feed, where it belongs. In FBS, the crushed oilseeds count as processing, and the cake is never counted at all.

The disadvantage of using SCL instead of FBS is coverage:
a. SCL starts in 2010.
b. A few countries are missing, because FAO has not compiled them (Japan, Sudan, Somalia and a few others).
c. SCL has no fish or seafood.

ASSUMPTIONS THAT GO INTO THE CALCULATION
----------------------------------------
Each assumption starts with a line on its scale: how much it changes the results.

1. For each item, supply equals the sum of its uses, in tonnes, and an element that SCL does not report is zero.
   >> Scale: major. The identity is the backbone of every table.

   For every item, country and year, SCL reports, in tonnes:
       production + imports - exports - stock variation
         = food + feed + seed + processing + other uses + losses + tourist consumption + residuals.

   When SCL does not report an element for an item, we treat it as zero. FAO builds each balance as a whole, so an
   element missing from a balance is almost always one that does not apply to the item. If a real value were
   missing, the item's balance would not close, and the step stops when more than 1% of the item balances do not
   close.

2. Densities are calculated from FAO's own food figures, and food keeps the calories and protein that FAO reports.
   >> Scale: major. These densities convert every stage except food.

   SCL reports every element in tonnes. Food is the exception: it is also reported in calories (the element
   "Calories/Year") and in protein ("Proteins/Year").

   The density of an item is its food nutrient divided by its food quantity.
   By food nutrients we mean either calories or proteins.
   The resulting density is measured in kilocalories per 100g or in grams of protein per 100 g.
   For all 416 SCL items with a food use, FAO's food nutrients are its 2024 nutrient
   conversion table times the item's edible-portion coefficient, the share of the item that people eat:
   https://www.fao.org/food-composition/tables-and-databases/detail/global-nutrient-conversion-table-for-fao-supply-utilization-accounts/en

   FAO uses one density per item in every country and year: for example, 334 kcal per 100 g for wheat grain, 348 for
   maize and 64 for potatoes. There are only nine exceptions to this: those nine items have a density of their own in a few countries, such as potatoes in Peru (91 kcal per 100 g, instead of the usual 64 kcal) and milled rice in Bangladesh (360, instead of 349).

   We use that density to convert all other elements of an item: production, imports, exports, stock variation, seed,
   losses, other uses, processing, feed, tourist consumption, and residuals.

   Where FAO reports a food nutrient for an item, food keeps that value. Where FAO reports no food nutrient, food is converted like all other elements, as quantity times density.

   Any gap between the converted stages and food goes to data adjustments (see assumption 9).

   The mass table does not need any densities: SCL already reports everything as mass, and we only convert tonnes to
   kilograms.

3. Each item has one density, used in every country and year (except for nine country-specific densities).
   >> Scale: minor. The item density is used for 90% of country-items (70% of tonnage). The nine country-specific
   densities are used for 0.01% of country-items (0.3% of tonnage).

   FAO uses the same density for an item in every country and year. So each item gets one density: its food
   nutrient divided by its food tonnes, both summed over all countries and all years.

   FAO makes nine exceptions, which we list in the items file. Each of them uses its own density of each year, and is
   left out of the item's sums.

4. Fish get a different density each year.
   >> Scale: minor. Fish carry 1.5% of tonnage.

   SCL does not have fish data, so we import it from FBS.
   FBS reports fish as groups, such as pelagic fish or demersal fish. The species in a group change from year to
   year, and so does the density: Iceland's pelagic fish ranges from 77 to 86 kcal per 100 g. So fish keep the
   density of each year. A year with no usable food takes the country's density over all years, whatever the tonnage,
   and a country with no usable food of a fish group takes the item density of that group.

5. Items that people hardly eat get fixed densities, and items that are never food are excluded.
   >> Scale: major for the stages "feed" and "processing_net". The fixed densities carry only 4.2% of tonnage, but
   that tonnage is the oilseed cakes and brans. Leaving out fuel ethanol is minor for World, but large in a few
   countries (it avoids processing creating 300 kcal per person per day in Brazil, and 1,500 kcal of fuel imports in
   the Netherlands).

   Cakes, brans, gluten feed and oil refining residues are hardly eaten, so their food cannot give a density. We give
   them fixed values instead, from three sources:
   a. FAO's 2024 nutrient conversion table, where it has the item: the cereal brans, centrifugal cane sugar and
      tallow.
   b. Feedipedia, for the oilseed cakes and gluten feed that FAO's table does not cover. Feedipedia collects the feed
      tables of INRAE, CIRAD, AFZ and FAO. Each item takes the composition of one matching table, converted to energy
      with the formula FAO uses for its own table: 4 kcal per gram of protein, 9 per gram of fat, 4 per gram of
      available carbohydrate and 2 per gram of fibre. That puts each cake on the same footing as the crop and the oil
      it comes from. Soybean cake, for example, gets 307 kcal and 43.4 g of protein per 100 g, from Feedipedia's
      soybean meal of 44% to 46% protein: the type that includes the hulls, as SCL's crushing yields imply.
   c. Uncited values, for a few minor items with no single published composition.
   The values, their sources and the arithmetic are in `food_supply_chain_scl.items.yml`.

   Listed items always use their fixed densities, so that a few hundred tonnes of soybean cake eaten somewhere cannot
   set the density of hundreds of millions of tonnes.

   Some items are never food in any form, so we leave them out of all three tables: fuel ethanol (undenatured
   alcohol of 80% or more), and a few others listed in the items file.

   Ethanol is the one that matters. The crops sent to distilleries already leave the chain where FAO records them:
   as other uses of the crop in Brazil and the United States (2,900 kcal per person per day of cane and molasses in
   Brazil in 2023), or as processing of molasses in Pakistan and India. Counting the ethanol itself as a processed
   product would cause three errors:
   a. Its calories would be credited back into processing, making processing appear to create calories (300 kcal per
      person per day in Brazil).
   b. The same calories would be counted again, as ethanol exports and ethanol burned.
   c. The fuel that importing countries buy would enter their chain as imports (1,500 kcal per person per day in the
      Netherlands).

6. Crops that are mostly processed are valued by the products that the world's factories make from them.
   >> Scale: major for the stages "crop_production" and "processing_net". These values carry 24% of tonnage: sugar
   cane, sugar beet, paddy rice, palm kernels, oil palm fruit, rapeseed, cotton seed, sunflower seed, coconuts, olives
   and maize germ.

   FAO's table values each crop at the part that people eat, through its edible-portion coefficient: sunflower seed
   without its hull, sugar cane as its juice, coconuts as the meat and water of a mix of young and mature nuts, paddy
   as husked rice, rapeseed as 38% oil and nothing else, and cotton seed as its kernels.

   That is the right value for the little that people eat as harvested. But almost all of these crops go to
   factories, which use the whole crop and make recorded products, such as cake, bran and molasses, from the parts
   FAO leaves out. At FAO's values, processing would appear to create calories wherever these crops are crushed or
   milled (sunflower seed in Ukraine, coconuts in the Pacific islands). And cotton seed, valued as kernels with
   nothing for the hulls, would show large losses.

   So we value these eleven crops by their products. Each year, a crop's value is the nutrient in everything the
   world's factories make from it, divided by the tonnes of the crop that the world processes. That World value is
   applied to every country. Four rules define what counts:
   a. Products that are processed further within the family are netted out, so they are not counted twice: copra,
      for example, pressed into coconut oil.
   b. Each family ends at the first products made from the crop. What is made from those products later keeps its
      own density elsewhere in the chain, but does not change the crop's value:
      - Sugar cane and sugar beet end at sugar and molasses. Raw sugar refined into white sugar is netted out, but
        refined sugar sent into drinks or sweets keeps its value as sugar. Sweets, syrups and flavored sugar are left
        out, because their other ingredients do not come from cane or beet.
      - Paddy rice ends at grain and bran products. Husked rice milled into grain, and bran pressed into oil and
        cake, are netted out. Grain sent on to become broken rice, flour, starch or drinks keeps its value as grain.
   c. Sugar cane and sugar beet share their products (raw and refined sugar, molasses), so they get one value
      together: 59 kcal per 100 g in 2023.
   d. Palm kernels are made from oil palm fruit and then crushed for their oil. So they are valued first, and the oil
      palm fruit family then counts its kernels at their new value.

   The families, their products and their order are in the items file. They follow FAO's commodity trees (FAO 2000,
   Technical Conversion Factors for Agricultural Commodities).

7. Each item's production counts as crops, animal products or processed products, following FAO's item groups.
   >> Scale: major. Together with the densities, the roles shape the chain: they decide which stage each tonne of
   production lands in.

   Adding up the production of all items would count the same calories many times, because sugar is made from sugar
   cane and bread from wheat. So each item has a role, which decides where its production goes. FAO's own item
   groups put every SCL item in one of four groups ("Crops, primary", "Livestock primary", "Crops processed" and
   "Livestock processed"), which map to three roles:
   a. "crop": crop production is the stage "crop_production", the start of the chain.
   b. "animal": the stage "feed" subtracts the crops fed to animals, and the stage "animal_products" adds back the
      meat, milk and eggs that the animals produce. The production of animal items is that second stage.
   c. "processed": factories turn crops into sugar, oils, flour and other products. The stage "processing_net" is
      what goes into factories (the "processing" of all items) minus what comes out (the "production" of processed
      items). So it counts only the calories that enter factories and do not come back as food.

   One override, in the items file: cotton seed counts as a crop. It comes from ginning seed cotton, but seed cotton
   is a fibre crop and not an SCL item, so cotton seed is the first point where the calories of the cotton harvest
   appear.

8. Fish and seafood come from FBS.
   >> Scale: minor for World (fish and fish oil are 46 of the 635 kcal per person per day of animal products in 2023,
   and 6 of the 43 g of protein); large for fishing nations.

   SCL has no fish or seafood, so we add the fish items of FBS for the countries and years that SCL covers. They go
   through the same rules as any other item, with two differences in how we read them:
   a. Stock variation is derived from FBS's domestic supply: production, plus imports, minus exports, minus domestic
      supply.
   b. FBS rounds its tonnes to the nearest 1,000 tonnes, which is too coarse to give a density in small countries. So
      the food tonnes used for fish densities come from FBS's food per person per year, multiplied by population.
      Calories and protein come from FBS's per-person figures in the same way.

   FAO has carried most fish rows forward unchanged since 2019 to 2021: they are identical in 2021, 2022 and 2023 for
   186 of 198 countries. So the fish figures of recent years are older than the rest of the chain.

   Fish oil ("Fish, Body Oil" and "Fish, Liver Oil") has the role "animal", like the fish it comes from, for two
   reasons:
   a. The FBS fish rows record no processing. The fish ground into meal and oil are inside the fish row's production,
      and leave inside its exports and feed, as live weight: Peru's pelagic fish row exports 5.2 million tonnes
      against 0.5 million eaten, because its fishmeal is counted as fish. With the role "processed", the oil would
      come out of processing with no input, and processing would appear to create calories (2,993 kcal per person
      per day in Iceland).
   b. Most of the oil's energy is not in the fish row. FAO values fish at the energy of their edible flesh: about 79
      kcal per 100 g of live anchoveta in Peru. That is about what the meal made from it holds (77 kcal), while the
      oil holds another 40. Counting the oil as an animal product adds those calories, and its exports, feed and
      industrial uses then leave the chain like any other item's. Imported oil is in no fish row of the importer
      (Norway imports 987 kcal per person per day of oil, against 426 of pelagic fish), so it is a real inflow.
   Protein does not change, since the oils have none. The costs of this choice are under KNOWN PROBLEMS.

9. Any gap between the other stages and food is counted as data adjustments.
    >> Scale: minor for World (10 kcal per person per day in 2023). Large where people eat coconuts: 14% of food in
    Micronesia in 2023, and 7% to 10% in Vanuatu, the Marshall Islands and Kiribati. Rounded fish tonnes can also
    matter in small countries.

    The stage "data_adjustments" holds everything that makes the chain end exactly on food:
    a. FAO's own "residuals".
    b. Rounding. FAO rounds tonnes, so the balances do not close exactly. Food keeps FAO's reported nutrients
       (assumption 2), while the other stages use the rounded tonnes. This matters most for fish (assumption 8).
    c. A difference of valuation, for the crops of assumption 6 that people both eat as harvested and process: mostly
       coconuts and sugar cane, which people chew. The tonnes that people eat leave the earlier stages at the crop's
       value under assumption 6 (140 kcal per 100 g of coconuts in 2023), but arrive in food at FAO's value for the
       part people eat (85 kcal). For World, this is about 10 kcal per person per day (coconuts 4.5, sugar cane 4.3);
       in Micronesia, it is 363 kcal.
    d. In energy and protein, any gain in processing (assumption 14).
    e. For World, the gap between imports and exports, and tourist consumption (assumption 10).
    Points b and c together are also kept as "balancing_difference", for quality control.

10. World exports equal World imports, and World tourist consumption is zero.
    >> Scale: minor, and only for World: the trade gap is a few percent of imports (at most 9%), and World tourist
    consumption is at most 0.21% of food.

    The world as a whole trades with no one, so World imports and exports should be equal. In the data they differ,
    because each is the sum of what countries report. We set World exports equal to World imports, which FAO
    considers the better-documented side (FAO 2025, Food Balance Sheets and Supply Utilization Accounts Resource
    Handbook, section 6.1), and the difference goes to "data_adjustments". Other regions do trade with the rest of
    the world, so we leave them as they are.

    Tourist consumption is food eaten in a country by people who live in another country. Every visitor lives in some
    country, so the world as a whole has no visitors, and World tourist consumption should be zero. In the data it is
    1 to 6 kcal per person per day from 2010 (the first year FAO reports it), or 0.05% to 0.21% of World food. We set
    it to zero, and the difference goes to "data_adjustments". Countries and other regions keep their tourist
    consumption, because for them it is real.

11. Each region is the sum of its countries.
    >> Scale: major for the 10 region aggregates, which only exist through this assumption; no country's values
    change.

    We drop FAO's own regional aggregates, and build OWID regions (World, continents, income groups and the European
    Union) from the member countries that have an SCL balance that year. Each stage of a region, in each unit, is the
    sum of that stage over those countries (fish included). The region's population is the sum of the same
    countries' populations.

    Regions have no densities of their own: each country's tonnes are converted with that country's densities, and
    the region adds up the results. A country has either a full balance or none, so the stages and the population
    always cover the same countries.

12. A region is dropped in years when its countries with data hold less than 80% of its population.
    >> Scale: minor. It removes a few region-years and one region; no country's values change.

    We drop a region-year when the countries with a balance hold less than 80% of the region's population, so that a
    value labeled "Africa" is never built from a small part of Africa. "Low-income countries" is left out altogether:
    SCL never covers more than 80% of its population, because FAO has not compiled Sudan, Somalia and a few others.

13. All values are per person per day.
    >> Scale: minor. A choice of unit; it rescales all values equally.

    All stages are divided by the population of assumption 11 and by 365 days.

14. Processing never adds calories or protein: any gain is moved to data adjustments.
    >> Scale: major for a few countries, nil for World. In 2023, 46 of 176 countries would otherwise show processing
    adding energy, 6 of them by more than 10% of food: Guyana 64%, Ukraine 14%, Uruguay 14%, the Netherlands 13%,
    Honduras 12% and Papua New Guinea 10%. In protein, 31 countries would, 4 of them by more than 10% (Guyana, New
    Zealand, Bolivia and Latvia). World shows a loss in both, so it is unchanged.

    A mill, a crusher or a brewery cannot put more energy or protein into its products than came in. Where the
    converted products exceed the converted inputs, the excess is a failure of the data or of the densities (the
    causes are under KNOWN PROBLEMS), not a feature of the country's food chain. A positive processing bar would tell
    readers that mills make food.

    So we move the gain to "data_adjustments", which exists for exactly this (assumption 9), and "processing_net"
    shows zero. Only gains move. The same causes also inflate or shrink the real losses of other countries, and
    those stay in processing, which remains the least certain stage.

    The rule applies to each country and region in each year, after the regions are summed from their countries. So
    a region with a gain is treated like a country: Oceania in every year in energy; South America, Europe, North
    America and the European Union in one year each; and Oceania in 2012 in protein. A region's processing is then
    no longer the sum of its countries', but processing plus data adjustments still is.

    The mass table is left alone. In tonnes, a gain in processing can be real, because breweries and bottlers add
    water: in Namibia, 22 thousand tonnes of malt become 174 thousand tonnes of beer. So processing there is the net
    change in weight, in either direction.

KNOWN PROBLEMS, NOT YET RESOLVED
--------------------------------
1. Processing would create calories in some countries. We move the gains to data adjustments, but their causes
   remain.
   >> Scale: major for a few countries. In 2023, the gains moved by assumption 14 exceed 10% of food in six countries
   in energy (Guyana 64%, Ukraine, Uruguay, the Netherlands, Honduras and Papua New Guinea) and in four in protein
   (Guyana, New Zealand, Bolivia and Latvia). 29 of 176 countries have a gain above 1% of food in energy, and 20 in
   protein; 20% of all country-years have some gain in energy, and 14% in protein. Small for World, where processing
   loses 106 kcal per person per day, 1.8% of crop production.

   Normally, "processing_net" is positive: more goes into factories than comes out. Assumption 14 keeps the gains
   out of the processing bar, but not out of the chart: they appear in data adjustments as calories entering the
   chain (more than half of food in Guyana). The same causes also distort the losses that stay in processing
   elsewhere. The gains have four causes:
   a. FAO's own quantities are inconsistent. Guyana's rice bran is 30% of the paddy it mills, so Guyana's rice
      creates about 2,400 kcal per person per day in 2023. In 2022, Bolivia's soybean cake weighs more than the
      beans crushed. We correct one such error, in `food_supply_chain_scl.corrections.yml`: since 2021, FAO imputes
      Denmark's molasses production as equal to its whole beet harvest. That made processing create about 2,900 kcal
      per person per day in Denmark, and about 40 in the European Union, and inflated feed by the same amounts.
   b. Factories that make more from a crop than the world average of assumption 6 (see problem 2): oil palm in
      Honduras and Papua New Guinea, sunflower seed in Ukraine, rice in Uruguay, and sugar beet in Ukraine, the
      Netherlands and Germany (beet holds more sugar than cane, and the two share one value).
   c. FAO's density for a raw item outside assumption 6 is below what its products contain: raw milk against its
      powders and casein (New Zealand in protein, Uruguay in energy).
   d. Products with no recorded input: hydrogenated oils, margarine and fat preparations (the Netherlands), and
      oilseed flours (Bolivia, in protein).

2. The values of assumption 6 are World averages, applied to every country.
   >> Scale: minor for World by construction; major for a few countries whose factories differ from the world
   average.

   A country whose factories make more from a crop than the world average shows a processing gain. Ukraine's
   sunflower mills make 535 kcal per 100 g of seed, against the World value of 507: a gain of 258 kcal per person per
   day in 2023. In Honduras, oil palm mills make 290 against 214.

   A country whose factories make less shows the difference as a processing loss. In Tonga and Micronesia, coconuts
   yield 109 and 97 kcal of copra and oil per 100 g of nuts, against 140: a loss of about 500 kcal per person per day.
   In Malaysia, oil palm yields 205 against 214: a loss of 670 kcal.

   Both mix real differences between factories with differences in what FAO records.

   Ukraine's chart is also dominated by FAO's own residuals, which are not ours to fix: 818 kcal per person per day in
   2023, and about 3,200 in 2020, when FAO records 12.9 million tonnes of maize, sunflower seed and wheat as used
   without a matching supply.

3. The stage "feed" only counts feed that passes through the balance.
   >> Scale: "animal_products" exceeds "feed" by more than 1 kcal in 14% of country-years in energy, and by more
   than 0.01 g in 24% in protein. World still has more recorded feed than animal output: 2,047 against 635 kcal in
   2023, and 103 against 43 g of protein.

   Grass, pasture and forage are not SCL items, so everything that grazing animals eat from pasture is missing from
   the chain. Wild-caught fish count as animal products with no feed at all. So in fishing and grazing countries
   (Iceland, Mongolia and several island states), "animal_products" can exceed "feed". The balance alone cannot tell
   how much of a country's gap these missing inputs explain.

4. Fish oil is partly counted twice: in calories where the food fish are fatty, and in tonnes everywhere.
   >> Scale: minor for World (1.3 of 4.1 kcal per person per day of fish oil in 2018); large in a few oil producers
   (all of Denmark's 285 kcal per person per day of oil production in 2018). In tonnes, 1.6% to 5% of the animal
   products of the oil producers. Protein is unaffected.

   A fish row carries about the energy of the fishmeal only where the food fish are lean (79 kcal per 100 g of live
   weight in Peru, 83 in Chile, 76 in Morocco). Where people eat fatty fish, the row's density already exceeds the
   meal's energy, so the oil pressed from the fish is counted a second time. That covers all of Denmark's 285 kcal
   per person per day of oil production in 2018, half of Norway's 279, and nearly all of the small production of
   Latvia, Japan, Spain, Sweden and Ireland.

   In tonnes, the oil is inside the fish tonnes in every producer, so the mass table counts it twice: 1.6% of Peru's
   animal products, 3% of Chile's and 5% of Mauritania's.

   We weighed two alternatives and rejected both:
   a. Leaving the oil out would miss about 70% of its energy (20% of Peru's animal products), and the oil that
      importers buy.
   b. Counting it in calories but not in tonnes would be exact, but would make one table an exception.

   FAO's oil figures exceed what the fish rows can yield in Iceland (13% of pelagic production), Denmark (17%) and
   Mauritania (11%), against about 5% for whole fish. Those are data problems, not method problems.

5. FAO records the crops sent to distilleries inconsistently, and fuel ethanol is left out (assumption 5).
   >> Scale: minor for World; above 10% of food in a few countries.

   a. Where FAO records the distillery crops as processing, the calories turned into fuel appear as a processing loss
      rather than under other uses. This happens in 22 of the 49 countries that produced more than 20 kcal per person
      per day of ethanol in 2023 (molasses in Pakistan, India, Mauritius and South Africa; cereals in Belgium,
      Slovakia, Moldova and others). The amount exceeds 10% of food in Eswatini, Hungary, Paraguay, Slovakia,
      Tajikistan, Belgium and Moldova.
   b. FAO records about 6,300 tonnes a year of this ethanol as food in Liberia, so Liberia loses 22 kcal of food (1%).
      It is the only country whose food changes.
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
# FBS elements used for the fish items (see assumption 8).
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
# The three item roles of assumption 7. The production of crops starts the chain as "crop_production", the
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
# Unit conversions. Exact by definition; they change no data beyond the choice of unit (assumption 13).
HUNDRED_GRAMS_PER_TONNE = 10_000
KG_PER_TONNE = 1000
GRAMS_PER_TONNE = 1_000_000
DAYS_PER_YEAR = 365

# SCL covers 2010 onward; the FBS fish items of assumption 8 are cut to the same years.
FIRST_YEAR = 2010

# Checks only; these thresholds change no data, they only decide when the step crashes.
# Tolerance of the identity check, per item balance: 1% of the summed uses plus FAO's rounding (checks
# assumption 1).
IDENTITY_RELATIVE_TOLERANCE = 0.01
IDENTITY_ABSOLUTE_TOLERANCE_TONNES = 2000
# Expected outcome of the coverage rule below; empty, because "Low-income countries" is left out altogether
# (checks assumption 12).
REGIONS_WITH_LOW_COVERAGE = set()
# Safety limit for assumption 10: World trade is only equalized while the gap is small; a gap above this share
# of imports crashes the step instead.
MAX_WORLD_TRADE_GAP = 0.2
# Safety limit for assumption 10: World tourist consumption is only set to zero while it is small (at most 0.21%
# of food in practice); a larger share of food crashes the step instead.
MAX_WORLD_TOURIST_CONSUMPTION = 0.005
# Checks assumption 3: every country-year with at least this much food of an item must have the item's density, within
# 1% of it, but never less than 0.5 kcal or 0.05 g of protein per 100 g, for items with very small densities (such as
# tea). Below this much food, FAO's rounding moves a year's density by more than the tolerance. The country-specific
# densities of the items file are exempt.
MIN_FOOD_TONNES_FOR_DENSITY_CHECK = 1000
DENSITY_RELATIVE_TOLERANCE = 0.01
DENSITY_ABSOLUTE_TOLERANCE = {"energy": 0.5, "protein": 0.05}
# Check the fixed factors of assumption 5 against the inferred factor where an item has substantial food use.
# This threshold controls only the check; every listed item uses its fixed factor.
MIN_FOOD_SHARE_FOR_FIXED_DENSITY_CHECK = 0.01

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
    # "Low-income countries" is left out (assumption 12): SCL covers at most 80% of its population.
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
# The items file (assumptions 5 to 8).
# --------------------------------------------------------------------------------------------------------------------
def load_manual_inputs() -> dict:
    """Load the items file: fixed densities, never-food items, implied-density families, overrides, fish, checks.

    Implements assumptions 5 and 6 (their manual inputs), plus the role override of assumption 7 and the fish list
    of assumption 8.
    """
    with open(paths.side_file("food_supply_chain_scl.items.yml")) as f:
        config = yaml.safe_load(f)
    expected = {
        "fixed_densities",
        "never_food",
        "output_implied_densities",
        "role_overrides",
        "fish_from_fbs",
        "country_specific_densities",
        "processing_families_for_checks",
    }
    assert set(config) == expected, f"Unexpected top-level keys in items file: {set(config) ^ expected}"
    for entry in config["fixed_densities"]["families"] + config["fixed_densities"]["items"]:
        assert {"energy", "protein"} <= set(entry), f"Fixed densities need energy and protein: {entry}"
    return config


# --------------------------------------------------------------------------------------------------------------------
# Roles (assumption 7).
# --------------------------------------------------------------------------------------------------------------------
def load_roles(tb_groups: Table) -> pd.Series:
    """Map each SCL item code to its role, from FAO's item groups.

    Implements assumption 7 (the roles).
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
    """Check the element units of assumption 2, the role coverage of assumption 7, and the items file names."""
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
    listed = manual["fixed_densities"]["items"] + manual["never_food"] + manual["role_overrides"]
    for item in listed + manual["country_specific_densities"]:
        named[item["code"]] = item["name"]
    countries = set(tb["country"].astype(str))
    unknown = [e["country"] for e in manual["country_specific_densities"] if e["country"] not in countries]
    assert not unknown, f"Country-specific densities name countries that are not in SCL: {unknown}"
    families = manual["output_implied_densities"]
    for i, family in enumerate(families):
        named.update(family["crops"])
        named.update(family["products"])
        assert set(family["intermediates"]) <= set(family["products"]), "Intermediates must be among the products."
        # A crop that another family makes (palm kernels, made from oil palm fruit) must be valued first, so that the
        # other family counts it at its new value (assumption 6).
        made_earlier = {code for earlier in families[:i] for code in earlier["products"]}
        assert not set(family["crops"]) & made_earlier, f"Move the family of {family['crops']} up in the items file."
    wrong = {c: (n, table_items.get(_pad_code(c))) for c, n in named.items() if table_items.get(_pad_code(c)) != n}
    assert not wrong, f"Manual inputs name items that do not match SCL (code: (expected, found)): {wrong}"


# --------------------------------------------------------------------------------------------------------------------
# The balance table (assumption 1).
# --------------------------------------------------------------------------------------------------------------------
def prepare_balance_table(tb: Table, roles: pd.Series, manual: dict) -> Table:
    """One row per (country, year, item) with one column per element, for all SCL items.

    Implements assumption 1 (missing elements are treated as zero); the rest is reshaping, with no data decision.
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

    Implements the never-food part of assumption 5.
    """
    never = [_pad_code(item["code"]) for item in manual["never_food"]]
    return tb[~tb["item_code"].isin(never)].reset_index(drop=True)


# --------------------------------------------------------------------------------------------------------------------
# Fish from FBS (assumption 8).
# --------------------------------------------------------------------------------------------------------------------
def prepare_fish_table(tb_fbsc: Table, manual: dict, population: Table) -> Table:
    """FBS fish items, reshaped like the SCL balance table.

    Implements assumption 8.
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
    # (kcal per year; tonnes of protein per year; tonnes of food) so that they can be summed over years and countries.
    tb = tb.merge(population, on=["country", "year"], how="inner")
    # Multiplying by population would copy population's display settings (rounding, projection flag) into every
    # stage, through the densities.
    tb["population"].metadata.display = None
    tb["food_kcal_per_year"] = tb["food_kcal_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    tb["food_protein_g_per_year"] = tb["food_protein_g_per_capita_per_day"] * DAYS_PER_YEAR * tb["population"]
    tb["food_tonnes_for_density"] = tb["food_kg_per_capita_per_year"] * tb["population"] / KG_PER_TONNE
    return tb[BALANCE_COLUMNS + ["population"]]


# --------------------------------------------------------------------------------------------------------------------
# Regions (assumptions 11 and 12).
# --------------------------------------------------------------------------------------------------------------------
def add_region_aggregates(chain: Table) -> Table:
    """Add the OWID regions as the sum of their countries' stages, and drop the region-years with low coverage.

    Implements assumptions 11 and 12.
    """
    assert not chain["country"].isin(REGIONS).any(), "Region rows must be dropped before aggregating."
    stages = [c for c in chain.columns if c not in ["country", "year", "population"]]
    flows = paths.regions.add_aggregates(
        chain[["country", "year"] + stages],
        regions=REGIONS,
        index_columns=["country", "year"],
        aggregations={c: "sum" for c in stages},
    )
    # Assumption 12 rides on this call: min_frac_population gates the summed population itself, so a region-year
    # whose members with a balance hold less than MIN_FRACTION_POPULATION_COVERED of the region's population (taken
    # from the population dataset) comes back with a nan population.
    population = paths.regions.add_aggregates(
        chain[["country", "year", "population"]],
        regions=REGIONS,
        index_columns=["country", "year"],
        aggregations={"population": "sum"},
        min_frac_population=MIN_FRACTION_POPULATION_COVERED,
    )
    chain = flows.merge(population, on=["country", "year"], how="left")

    # Drop the gated region-years, so that a value labeled "Africa" is never built from a small fraction of Africa.
    dropped = chain.loc[chain["population"].isnull(), ["country", "year"]].drop_duplicates()
    assert set(dropped["country"]) <= set(REGIONS), "A country lost its population in the region aggregation."
    assert set(dropped["country"]) == REGIONS_WITH_LOW_COVERAGE, (
        f"Regions dropped for low coverage changed: {sorted(set(dropped['country']))}. Update the docstring."
    )
    return chain[chain["population"].notnull()].reset_index(drop=True)


def sanity_check_balance_identity(tb: Table) -> None:
    """Check assumption 1: SCL balances close in tonnes."""
    supply = tb["production"] + tb["imports"] - tb["exports"] - tb["stock_variation"]
    uses = tb[USES].sum(axis=1)
    tolerance = IDENTITY_RELATIVE_TOLERANCE * uses.abs() + IDENTITY_ABSOLUTE_TOLERANCE_TONNES
    share_open = ((supply - uses).abs() > tolerance).mean()
    assert share_open < 0.01, f"Supply differs from the sum of uses in {100 * share_open:.2f}% of item balances."


# --------------------------------------------------------------------------------------------------------------------
# Densities (assumptions 2 to 6).
# --------------------------------------------------------------------------------------------------------------------
def fixed_density_map(tb: Table, manual: dict, nutrient: str) -> dict[str, float]:
    """Fixed densities per item code for one nutrient, from the families (by name pattern) and the explicit items.

    Implements assumption 5.
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

    Implements assumption 6, with the densities already in `tb`. The ratio is summed over all countries, so that a
    family uses the values that earlier families wrote into `tb`. The nutrient of intermediates processed further
    within the family is taken out, since it would otherwise count twice: as their own production and as the
    production of what they become.
    """
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    crops = [_pad_code(c) for c in family["crops"]]
    products = [_pad_code(c) for c in family["products"]]
    intermediates = [_pad_code(c) for c in family["intermediates"]]
    members = tb[tb["item_code"].isin(crops + products)]
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

    Implements assumption 2 (densities from food), 4 (one factor per country and item), 5 (fish by year), 6 (fixed)
    and 7 (implied by products).
    """
    assert nutrient != "mass", "The mass table involves no density; it never goes through this function."
    assert not tb["country"].isin(REGIONS).any(), "Regions have no densities: they are sums of their countries' stages."
    tb = tb.copy()
    # The nutrient decides the ingredients (see NUTRIENTS): which food total is the numerator of the density, and
    # the physical ceiling.
    numerator = NUTRIENTS[nutrient]["numerator"]
    ceiling = NUTRIENTS[nutrient]["ceiling"]

    # Assumption 2: the density of a cell is its food nutrient (kilocalories, or grams of protein) over its food, in
    # 100 g portions. Cells with no food, no reported nutrient or a density above the physical ceiling (sub-tonne food
    # quantities, where FAO's rounding breaks the ratio) say nothing about the factor and are left out of the sums.
    cell = tb[numerator] / (tb["food_tonnes_for_density"] * HUNDRED_GRAMS_PER_TONNE)
    valid = np.isfinite(cell) & (cell >= 0) & (cell <= ceiling)
    nutrient_eaten = tb[numerator].where(valid, 0)
    tonnes_eaten = tb["food_tonnes_for_density"].where(valid, 0)
    # Assumptions 3 and 4: fish and the country-specific densities of the items file keep the density of each year; a
    # year with no usable food takes the country-item's density over all years.
    pair = [tb["country"], tb["item_code"]]
    pair_tonnes = tonnes_eaten.groupby(pair).transform("sum")
    pair_density = nutrient_eaten.groupby(pair).transform("sum") / (pair_tonnes * HUNDRED_GRAMS_PER_TONNE)
    is_fish = tb["item_code"].isin([_pad_code(item["code"]) for item in manual["fish_from_fbs"]])
    exceptions = {(e["country"], _pad_code(e["code"])) for e in manual["country_specific_densities"]}
    is_exception = pd.Series([key in exceptions for key in zip(tb["country"], tb["item_code"])], index=tb.index)
    yearly = is_fish | is_exception
    # Assumption 3: every other country uses the item's density: the item's food nutrient over its food tonnes, summed
    # over all countries (except the country-specific densities) and all years. Fish count towards their item's
    # density, which is only used for fish that a country has no usable food of.
    item = tb["item_code"]
    item_tonnes = tonnes_eaten.where(~is_exception, 0).groupby(item).transform("sum")
    item_density = nutrient_eaten.where(~is_exception, 0).groupby(item).transform("sum") / (
        item_tonnes * HUNDRED_GRAMS_PER_TONNE
    )
    item_density = item_density.where(item_tonnes > 0)
    # Every country-year with enough food must have the item's density; otherwise it is a new exception to list.
    tolerance = np.maximum(DENSITY_RELATIVE_TOLERANCE * item_density, DENSITY_ABSOLUTE_TOLERANCE[nutrient])
    tested = valid & ~yearly & (tb["food_tonnes_for_density"] >= MIN_FOOD_TONNES_FOR_DENSITY_CHECK)
    off = tested & ((cell - item_density).abs() > tolerance)
    assert not off.any(), (
        f"FAO's {nutrient} density differs from the item's density in country-items not listed as country-specific "
        f"in the items file: {sorted(set(zip(tb.loc[off, 'country'], tb.loc[off, 'fao_item'])))[:20]}"
    )
    own_year = cell.where(yearly & valid).fillna(pair_density.where(yearly & (pair_tonnes > 0)))
    tb["density"] = own_year.fillna(item_density)
    tb["density_source"] = np.select([own_year.notnull(), item_density.notnull()], ["own_year", "item"], default="none")

    # Assumption 5: listed items always get fixed factors. A few hundred tonnes of soybean cake eaten somewhere
    # should not set the density of hundreds of millions of tonnes. Where food use is substantial, check agreement
    # with the inferred factor before overriding it, using the same rounding tolerance as the other factor check.
    fixed = fixed_density_map(tb, manual, nutrient)
    is_fixed = tb["item_code"].isin(fixed)
    fixed_factor = tb["item_code"].map(fixed)
    supply = (tb["production"] + tb["imports"]).abs().groupby(tb["item_code"]).transform("sum")
    food_share = tb["food"].abs().groupby(tb["item_code"]).transform("sum") / supply
    tolerance = np.maximum(fixed_factor.abs() * DENSITY_RELATIVE_TOLERANCE, DENSITY_ABSOLUTE_TOLERANCE[nutrient])
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

    # Assumption 6: crops that go to factories are valued by what the world's factories make of them, each year.
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
    """Check the scales promised in assumption 3: every tonne has a density, and most tonnage uses FAO's factors."""
    ceiling = NUTRIENTS[nutrient]["ceiling"]
    assert (tb["density"] >= 0).all() and (tb["density"] <= ceiling).all(), f"{nutrient} densities out of range."
    mass = (tb["production"] + tb["imports"]).abs()
    provenance = mass.groupby(tb["density_source"]).sum()
    provenance = (100 * provenance / provenance.sum()).round(2)
    # Crops that go to factories (assumption 6) and items nobody eats (assumption 5) legitimately use other values.
    assert provenance.get("none", 0) == 0, "Some supply has no density."
    from_food = provenance.get("own_year", 0) + provenance.get("item", 0)
    assert from_food > 65, f"Only {from_food:.1f}% of supply uses a {nutrient} factor read from FAO's food data."


def sanity_check_processing_families(tb: Table, manual: dict, nutrient: str) -> None:
    """For all countries together in the latest year, nutrient out of processing as products vs nutrient in, per family."""
    latest = tb[tb["year"] == tb["year"].max()]
    for name, codes in manual["processing_families_for_checks"].items():
        members = latest[latest["item_code"].isin([_pad_code(c) for c in codes])]
        nutrient_in = (members["processing"] * members["density"]).sum()
        nutrient_out = (members.loc[members["role"] == "processed", "production"] * members["density"]).sum()
        ratio = nutrient_out / nutrient_in
        # Protein is lost more than energy in processing, because protein-rich by-products such as brewer's grains
        # are not SCL items; hence the looser band.
        low, high = (0.75, 1.15) if nutrient == "energy" else (0.5, 1.2)
        assert low < ratio < high, f"Processing family {name!r} ({nutrient}): out is {100 * ratio:.0f}% of in."


# --------------------------------------------------------------------------------------------------------------------
# The chain (assumptions 2, 7, 9, 10 and 13).
# --------------------------------------------------------------------------------------------------------------------
def convert_elements_to_nutrient(tb: Table, nutrient: str) -> Table:
    """Convert elements from tonnes; food keeps the nutrient FAO reports wherever FAO reports one.

    Implements the conversion half of assumption 2.
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

    Implements assumption 7. The output has one row per country and year, with one column per stage.
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


def move_processing_gains_to_adjustments(chain: Table) -> Table:
    """Move any gain in processing to "data_adjustments", so that processing never adds energy or protein.

    Implements assumption 14. Called for energy and protein only: in tonnes, a gain in processing can be real (water
    added in brewing and bottling).
    """
    gain = (-chain["processing_net"]).clip(lower=0)
    chain["processing_net"] = chain["processing_net"] + gain
    chain["data_adjustments"] = chain["data_adjustments"] - gain
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
def sanity_check_outputs(tb: Table, tb_fbsc: Table, nutrient: str) -> None:
    """Check one output table: shape, no negative magnitudes, the chain ends on food, and World matches FAO."""
    assert tb.columns[tb.isna().all()].empty, "Output has fully-nan columns."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    # Processing can gain weight (water added in brewing and bottling) but not energy or protein (assumption 14).
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
    # Assumption 11: FAO's own regional aggregates are dropped (OWID regions are built later from countries).
    tb_scl = tb_scl[~tb_scl["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    tb_fbsc = tb_fbsc[~tb_fbsc["country"].astype(str).str.contains("(FAO)", regex=False)].reset_index(drop=True)
    # Known FAOSTAT errors, patched on the input rows so that the regions built below inherit the fix (see the
    # corrections file for the reasons, and for why this is done here rather than in the garden SCL step).
    tb_scl = paths.apply_corrections(tb_scl)
    # Assumption 7: the role of every item, from FAO's item groups.
    roles = load_roles(tb_groups)
    sanity_check_inputs(tb_scl, roles=roles, manual=manual)

    # Assumption 1: the balance table, with missing elements as zero.
    tb = prepare_balance_table(tb_scl, roles=roles, manual=manual)
    # Assumption 5: items that are never food (castor, kapok, fuel ethanol...) are left out of all three tables.
    tb = drop_never_food_items(tb, manual=manual)
    population = tb[["country", "year", "population"]].drop_duplicates()
    # Assumption 8: fish and seafood, taken from FBS.
    tb_fish = prepare_fish_table(tb_fbsc, manual=manual, population=population)
    tb = pr.concat([tb, tb_fish], ignore_index=True)
    sanity_check_balance_identity(tb)

    tables = []
    for nutrient in NUTRIENTS:
        if nutrient == "mass":
            # The mass table involves no density: tonnes are converted to kilograms, nothing else.
            converted = convert_elements_to_nutrient(tb, nutrient=nutrient)
        else:
            # Assumptions 2 to 6: the density of every item: FAO's factors read from food, fixed and implied values.
            tb_nutrient = add_densities(tb, manual=manual, nutrient=nutrient)
            sanity_check_densities(tb_nutrient, nutrient=nutrient)
            sanity_check_processing_families(tb_nutrient, manual=manual, nutrient=nutrient)
            # Assumption 2: convert tonnes; food keeps FAO's reported nutrients wherever reported.
            converted = convert_elements_to_nutrient(tb_nutrient, nutrient=nutrient)
        # Assumption 7: production split by role, items summed into the stages of the chain.
        chain = sum_items_into_stages(converted, population=population)
        # Assumptions 11 and 12: OWID regions as the sum of their countries, dropping region-years with low coverage.
        chain = add_region_aggregates(chain)
        if nutrient != "mass":
            # Assumption 14: processing cannot create energy or protein; any gain goes to data adjustments.
            chain = move_processing_gains_to_adjustments(chain)
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
