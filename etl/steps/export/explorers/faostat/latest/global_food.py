"""Step that pushes the global-food explorer (tsv content) to DB, to create the global-food (csv-based) explorer.

NOTE: This script has been migrated and adapted from the old one in owid-content/scripts.
Ideally, the global food explorer would be an indicator-based explorer, instead of reading csv files.

This step takes three input files and combines them into a big explorer spreadsheet for the global food explorer.
The files are:
- (1) global-food-explorer.template.tsv: A template file that contains the header and footer of the spreadsheet, together with some placeholders.
    - The explorer title and subtitle
    - The default country selection
    - The column definitions of the data files, including source name, unit, etc.
    - Three special placeholders: `$graphers_tsv`, `table_defs`, `food_slugs`.
- (2) foods.csv: a list of foods and their singular and plural names. It has the following columns:
    | name       | description                                                                                                                                                                                                       |
    | :--------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
    | `slug`     | The slug needs to match the filename of the data file. E.g. if the data file is called `meat_rabbit.csv`, then the slug should be `meat_rabbit`.                                                                  |
    | `dropdown` | This is what is displayed in the food dropdown of the explorer.                                                                                                                                                   |
    | `singular` | The singular form of the food product, as used in titles. It should be in title case (i.e. written as seen in the beginning of a sentence) and should fit in a sentence such as `Land used for [...] production`. |
    | `plural`   | The plural form of the food product, as used in titles. It should be in title case (i.e. written as seen in the beginning of a sentence) and should fit in a sentence such as `Domestic supply of [...]`.         |
    | `_tags`    | The tags of this food product. This entry determines which views to show for this food product. See [Tags](#Tags) below.                                                                                          |
- (3) views-per-food.csv: a list of all available views for every food, including subtitle etc. The title can contain placeholders which are then filled out with the food name.
This is all further complicated by the fact that we have different tag for food products, which enable views with different columns, units and subtitles.
We take the cartesian product between (2) and (3) - according to the tag -, sprinkle some magic dust to make the titles work, and then place that massive table into the template (1).
Additionally, the `title` and `subtitle` columns support the following placeholders for the particular food name:
    - `${food_singular}` will be replaced by the singular version of the word as written in `foods.csv`, i.e. starting with an uppercase letter.
    - Example: `${food_singular} production` → `Apple production`
    - `${food_singular_lower}` will be replaced by the lowercase version of the singular given in `foods.csv`.
    - Example: `Land used for ${food_singular_lower} production` → `Land used for apple production`
    - `${food_plural}` will be replaced by the plural version of the word as written in `foods.csv`, i.e. starting with an uppercase letter.
    - Example: `${food_plural} used for animal feed` → `Apples used for animal feed`
    - `${food_plural_lower}` will be replaced by the lowercase version of the plural given in `foods.csv`.
    - Example: `Domestic supply of ${food_plural_lower}` → `Domestic supply of apples`

    There is a special `_tags` column that will not be part of the output `.explorer.tsv` file.
    It can contain a comma-separated list of [Tags](#Tags), specifying that the view will be available for all food products with this tag.

Both the `foods.csv` and `views-per-food.csv` files have a special `_tags` column.
In this, a comma-separated list of tags can be given that specifies which view will be available for which food product.
Let's see how this works based on an example:

`foods.csv`

| \_tags             | slug        |
| :----------------- | :---------- |
| qcl,fbsc,crop      | apples      |
| qcl,animal-product | meat_rabbit |
| fbsc,crop          | barley      |

`views_per_food.csv`

| \_tags         | title             |
| :------------- | :---------------- |
| qcl            | Production        |
| crop           | Yield [t/ha]      |
| crop           | Land use          |
| animal-product | Yield [kg/animal] |
| fbsc           | Imports           |

With this configuration, the following metrics would be available for the different foods:

- apples: Production, Yield [t/ha], Land use, Imports
- meat_rabbit: Production, Yield [kg/animal]
- barley: Yield [t/ha], Land use, Imports

Through the use of these tags, certain views can be enabled and disabled on a per-food basis.

"""

import textwrap
from io import StringIO
from pathlib import Path
from string import Template

import numpy as np
import pandas as pd
from owid.catalog import find
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV

# Initialize log.
log = get_logger()

CURRENT_DIR = Path(__file__).parent

TEMPLATE_CONTENT = """explorerTitle	Global Food
explorerSubtitle	Explore the world's food system crop-by-crop from production to plate.
isPublished	true
wpBlockId	46846
thumbnail	https://ourworldindata.org/app/uploads/2021/12/global-food-explorer-thumbnail.png
selection	United States	Germany	France	United Kingdom	Brazil	South Africa
yAxisMin	0
hasMapTab	true
tab	map
subNavId	explorers
subNavCurrentId	global-food
pickerColumnSlugs	country population production__tonnes production__kg__per_capita producing_or_slaughtered_animals__animals producing_or_slaughtered_animals__animals__per_capita yield__tonnes_per_ha yield__kg_per_animal area_harvested__ha area_harvested__m2__per_capita food_available_for_consumption__kg_per_year__per_capita food_available_for_consumption__g_per_day__per_capita food_available_for_consumption__kcal_per_day__per_capita food_available_for_consumption__protein_g_per_day__per_capita food_available_for_consumption__fat_g_per_day__per_capita imports__tonnes imports__kg__per_capita exports__tonnes exports__kg__per_capita domestic_supply__tonnes domestic_supply__kg__per_capita waste_in_supply_chain__tonnes waste_in_supply_chain__kg__per_capita food__tonnes food__kg__per_capita other_uses__tonnes other_uses__kg__per_capita feed__tonnes feed__kg__per_capita
graphers
$graphers_tsv

$table_defs
columns	$food_slugs
	slug	name	type	transform	shortUnit	unit	sourceName	dataPublishedBy	sourceLink
	product	Product	String
	country	Country	EntityName
	year	Year	Year
	population	Population	Population
	production__tonnes	Production (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	production__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	production__kg__per_capita	Production per capita (kg)	Numeric	multiplyBy production__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	yield__tonnes_per_ha	Yield (t/ha)	Numeric		t/ha	tonnes per hectare	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	yield__kg_per_animal	Yield (kg/animal)	Numeric		kg/animal	kilograms per animal	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	area_harvested__ha	Land Use (ha)	Integer		ha	hectares	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	area_harvested__ha__per_capita		Numeric		ha	hectares	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	area_harvested__m2__per_capita	Land Use per capita (m²)	Numeric	multiplyBy area_harvested__ha__per_capita 10000	m²	square metres	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	producing_or_slaughtered_animals__animals	Producing or slaughtered animals	Integer				UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	producing_or_slaughtered_animals__animals__per_capita	Producing or slaughtered animals per capita	Numeric				UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/QCL
	imports__tonnes	Imports (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	imports__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	imports__kg__per_capita	Imports per capita (kg)	Numeric	multiplyBy imports__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	exports__tonnes	Exports (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	exports__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	exports__kg__per_capita	Exports per capita (kg)	Numeric	multiplyBy exports__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	domestic_supply__tonnes	Domestic supply (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	domestic_supply__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	domestic_supply__kg__per_capita	Domestic supply per capita (kg)	Numeric	multiplyBy domestic_supply__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food__tonnes	Food (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food__kg__per_capita	Food per capita (kg)	Numeric	multiplyBy food__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	feed__tonnes	Animal feed (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	feed__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	feed__kg__per_capita	Animal feed per capita (kg)	Numeric	multiplyBy feed__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	other_uses__tonnes	Other uses (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	other_uses__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	other_uses__kg__per_capita	Other uses per capita (kg)	Numeric	multiplyBy other_uses__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	waste_in_supply_chain__tonnes	Supply chain waste (t)	Integer		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	waste_in_supply_chain__tonnes__per_capita		Numeric		t	tonnes	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	waste_in_supply_chain__kg__per_capita	Supply chain waste per capita (kg)	Numeric	multiplyBy waste_in_supply_chain__tonnes__per_capita 1000	kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food_available_for_consumption__kg_per_year__per_capita	Food supply (kg per capita per year)	Numeric		kg	kilograms	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food_available_for_consumption__g_per_day__per_capita	Food supply (g per capita per day)	Numeric	multiplyBy food_available_for_consumption__kg_per_year__per_capita 2.7397260	g	grams	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH	# 1000 / 365 = 2.7397260 (1000g/kg, 365 days/year)
	food_available_for_consumption__kcal_per_day__per_capita	Food supply (kcal per capita per day)	Numeric		kcal	kilocalories	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food_available_for_consumption__protein_g_per_day__per_capita	Food supply (Protein g per capita per day)	Numeric		g	grams	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
	food_available_for_consumption__fat_g_per_day__per_capita	Food supply (Fat g per capita per day)	Numeric		g	grams	UN Food and Agriculture Organization (FAO)	Food and Agriculture Organization of the United Nations (FAO) ($year)	https://www.fao.org/faostat/en/#data/FBS, https://www.fao.org/faostat/en/#data/FBSH
"""

FOODS_CONTENT = """slug,dropdown,singular,plural,_tags,note
almonds,Almonds,Almond,Almonds,production land_use crop_yield,
animal_fats,Animal fats,Animal fat,Animal fats,food_balances waste animal_feed,
apples,Apples,Apple,Apples,production land_use crop_yield food_balances waste animal_feed,
apricots,Apricots,Apricot,Apricots,production land_use crop_yield,
areca_nuts,Areca nuts,Areca nut,Areca nuts,production land_use crop_yield,"The areca nut is the seed of the areca palm, and is commonly referred to as betel nut. It is used as a carcinogenic drug in many cultures."
artichokes,Artichokes,Artichoke,Artichokes,production land_use crop_yield,
asparagus,Asparagus,Asparagus,Asparagus,production land_use crop_yield,
avocados,Avocados,Avocado,Avocados,production land_use crop_yield,
bananas,Bananas,Banana,Bananas,production land_use crop_yield food_balances waste animal_feed,
barley,Barley,Barley,Barley,production land_use crop_yield food_balances waste animal_feed,
beans_dry,"Beans, dry",Dry bean,Dry beans,production land_use crop_yield food_balances waste animal_feed,
beans_green,Green beans,Green bean,Green beans,production land_use crop_yield,"This crop refers to green beans, the vegetable product."
beeswax,Beeswax,Beeswax,Beeswax,production,
blueberries,Blueberries,Blueberry,Blueberries,production land_use crop_yield,
brazil_nuts_with_shell,"Brazil nuts, with shell",Brazil nut,Brazil nuts,production land_use crop_yield,
broad_beans,Broad beans,Broad bean,Broad beans,production land_use crop_yield,
buckwheat,Buckwheat,Buckwheat,Buckwheat,production land_use crop_yield,
buffalo_hides,Buffalo hides,Buffalo hide,Buffalo hides,production animals_slaughtered,
butter_and_ghee,Butter and ghee,Butter and ghee,Butter and ghee,food_balances waste animal_feed,
cabbages,Cabbages,Cabbage,Cabbages,production land_use crop_yield,
canary_seed,Canary seed,Canary seed,Canary seeds,production land_use crop_yield,
carrots_and_turnips,Carrots and turnips,Carrot and turnip,Carrots and turnips,production land_use crop_yield,
cashew_nuts,Cashew nuts,Cashew nut,Cashew nuts,production land_use crop_yield,
cassava,Cassava,Cassava,Cassava,production land_use crop_yield food_balances waste animal_feed,
castor_oil_seed,Castor oil seed,Castor oil seed,Castor oil seeds,production land_use crop_yield,
cattle_hides,Cattle hides,Cattle hide,Cattle hides,production animals_slaughtered,
cauliflowers_and_broccoli,Cauliflowers and broccoli,Cauliflower and broccoli,Cauliflowers and broccoli,production land_use crop_yield,
cereals,Cereals,Cereal,Cereals,production land_use crop_yield,"Cereals include wheat, rice, maize, barley, oats, rye, millet, sorghum, buckwheat, and mixed grains."
cheese,Cheese,Cheese,Cheese,production,
cherries,Cherries,Cherry,Cherries,production land_use crop_yield,
chestnut,Chestnuts,Chestnut,Chestnuts,production land_use crop_yield,
chickpeas,Chickpeas,Chickpea,Chickpeas,production land_use crop_yield,
chillies_and_peppers,Chillies and peppers,Chili and pepper,Chillies and peppers,production land_use crop_yield food_balances waste,
citrus_fruit,Citrus fruit,Citrus fruit,Citrus fruit,production land_use crop_yield,
cocoa_beans,Cocoa beans,Cocoa bean,Cocoa beans,food_balances waste animal_feed,
coconut_oil,Coconut oil,Coconut oil,Coconut oil,production food_balances waste,
coconuts,Coconuts,Coconut,Coconuts,food_balances waste animal_feed,
coffee_green,"Coffee, green",Green coffee,Green coffee,production land_use crop_yield,"Green coffee beans are coffee seeds (beans) that have not yet been roasted."
cotton,Cotton,Cotton,Cotton,production,
cottonseed,Cottonseed,Cottonseed,Cottonseeds,production food_balances waste animal_feed,
cottonseed_oil,Cottonseed oil,Cottonseed oil,Cottonseed oil,production food_balances waste,
cow_peas,Cow peas,Cow pea,Cow peas,production land_use crop_yield,
cranberries,Cranberries,Cranberry,Cranberries,production land_use crop_yield,
cucumbers_and_gherkins,Cucumbers and gherkins,Cucumber and gherkin,Cucumbers and gherkins,production land_use crop_yield,
currants,Currants,Currant,Currants,production land_use crop_yield,
dates,Dates,Date,Dates,production land_use crop_yield food_balances waste animal_feed,
eggplants,Eggplants (aubergine),Eggplant,Eggplants,production land_use crop_yield,
eggs,Eggs,Egg,Eggs,production animal_yield food_balances waste animal_feed,
eggs_from_hens,Eggs from hens,Hen egg,Hen eggs,production animal_yield,
eggs_from_other_birds_excl_hens,"Eggs from other birds (excl. hens)","Egg (from other birds, excl. hens)","Eggs (from other birds, excl. hens)",production animal_yield,
fat_buffaloes,"Fat, buffaloes",Buffalo fat,Buffalo fat,production animals_slaughtered,
fat_camels,"Fat, camels",Camel fat,Camel fat,production animals_slaughtered,
fat_cattle,"Fat, cattle",Cattle fat,Cattle fat,production animals_slaughtered,
fat_goats,"Fat, goats",Goat fat,Goat fat,production animals_slaughtered,
fat_pigs,"Fat, pigs",Pig fat,Pig fat,production animals_slaughtered,
fat_sheep,"Fat, sheep",Sheep fat,Sheep fat,production animals_slaughtered,
fibre_crops,Other fibre crops,Fibre crop,Fibre crops,production land_use crop_yield,
fish_and_seafood,Fish and seafood,Fish and seafood,Fish and seafood,food_balances animal_feed,
flax_raw_or_retted,"Flax, raw or retted","Flax, raw or retted","Flax, raw or retted",production land_use crop_yield,
fruit,Fruit,Fruit,Fruit,production land_use crop_yield food_balances waste animal_feed,
garlic,Garlic,Garlic,Garlic,production land_use crop_yield,
grapefruit,Grapefruit,Grapefruit,Grapefruits,production land_use crop_yield food_balances waste,
grapes,Grapes,Grapes,Grapes,production land_use crop_yield,
green_maize,Green maize,Green maize,Green maize,production land_use crop_yield,
groundnuts,Groundnuts,Groundnut,Groundnuts,production land_use crop_yield food_balances waste animal_feed,
groundnut_oil,Groundnut oil,Groundnut oil,Groundnut oil,production food_balances waste,
hazelnuts,Hazelnuts,Hazelnut,Hazelnuts,production land_use crop_yield,
hempseed,Hempseed,Hempseed,Hempseeds,production land_use crop_yield,
herbs_eg_fennel,Herbs (e.g. fennel),Herb (e.g. fennel),Herbs (e.g. fennel),production land_use crop_yield,
honey,Honey,Honey,Honey,production food_balances waste,
jute,Jute,Jute,Jute,production land_use crop_yield,
karite_nuts,Karite nuts,Karite nut,Karite nuts,production land_use crop_yield,
kiwi,Kiwi,Kiwi,Kiwi,production land_use crop_yield,
kola_nuts,Kola nuts,Kola nut,Kola nuts,production land_use crop_yield,
leeks,Leeks,Leek,Leeks,production land_use crop_yield,
lemons_and_limes,Lemons and limes,Lemon and lime,Lemons and limes,production land_use crop_yield food_balances waste,
lentils,Lentils,Lentil,Lentils,production land_use crop_yield,
lettuce,Lettuce,Lettuce,Lettuce,production land_use crop_yield,
linseed,Linseed,Linseed,Linseeds,production land_use crop_yield,
linseed_oil,Linseed oil,Linseed oil,Linseed oil,production,
maize,Maize (corn),Maize,Maize,production land_use crop_yield food_balances waste animal_feed,
maize_oil,Maize oil,Maize oil,Maize oil,production food_balances waste,
mangoes,Mangoes,Mango,Mangoes,production land_use crop_yield,
margarine,Margarine,Margarine,Margarine,production,
meat_total,"Meat, Total",Total meat,All meat,production animals_slaughtered food_balances waste animal_feed,
meat_ass,"Meat, ass",Ass meat,Ass meat,production animals_slaughtered animal_yield,
meat_beef_and_buffalo,"Meat, beef and buffalo",Beef and buffalo meat,Beef and buffalo meat,production animals_slaughtered animal_yield,
meat_beef,"Meat, beef",Beef,Beef,food_balances waste animal_feed,
meat_buffalo,"Meat, buffalo",Buffalo meat,Buffalo meat,production animals_slaughtered animal_yield,
meat_camel,"Meat, camel",Camel meat,Camel meat,production animals_slaughtered animal_yield,
meat_chicken,"Meat, chicken",Chicken meat,Chicken meat,production animals_slaughtered animal_yield,
meat_duck,"Meat, duck",Duck meat,Duck meat,production animals_slaughtered animal_yield,
meat_game,"Meat, game",Game meat,Game meat,production animals_slaughtered animal_yield,
meat_goat,"Meat, goat",Goat meat,Goat meat,production animals_slaughtered animal_yield,
meat_goose_and_guinea_fowl,"Meat, goose and guinea fowl",Goose and guinea fowl meat,Goose and guinea fowl meat,production animals_slaughtered animal_yield,
meat_horse,"Meat, horse",Horse meat,Horse meat,production animals_slaughtered animal_yield,
meat_lamb_and_mutton,"Meat, lamb and mutton",Lamb and mutton meat,Lamb and mutton meat,production animals_slaughtered animal_yield,
meat_mule,"Meat, mule",Mule meat,Mule meat,production animals_slaughtered animal_yield,
meat_pig,"Meat, pig",Pig meat,Pig meat,production animals_slaughtered animal_yield food_balances waste,
meat_poultry,"Meat, poultry",Poultry meat,Poultry meat,production animals_slaughtered animal_yield food_balances waste animal_feed,
meat_rabbit,"Meat, rabbit",Rabbit meat,Rabbit meat,production animals_slaughtered animal_yield,
meat_sheep_and_goat,"Meat, sheep and goat",Sheep and goat meat,Sheep and goat meat,production animals_slaughtered animal_yield food_balances waste animal_feed,
meat_turkey,"Meat, turkey",Turkey meat,Turkey meat,production animals_slaughtered animal_yield,
melon,Melon,Melon,Melon,production land_use crop_yield,
melonseed,Melonseed,Melonseed,Melonseeds,production land_use crop_yield,
milk,Milk,Milk,Milk,production animal_yield food_balances waste animal_feed,"Milk represents the raw equivalents of all dairy products including cheese, yoghurt, cream and milk consumed as the final product."
millet,Millet,Millet,Millet,production land_use crop_yield food_balances waste animal_feed,
mixed_grains,Mixed grains,Mixed grain,Mixed grains,production land_use crop_yield,
molasses,Molasses,Molasses,Molasses,production,
mushrooms,Mushrooms,Mushroom,Mushrooms,production land_use crop_yield,
mustard_seed,Mustard seed,Mustard seed,Mustard seeds,production land_use crop_yield,
nuts,Nuts,Nut,Nuts,food_balances waste,"Nuts is the sum of all nut crops including brazil nuts, cashews, almonds, walnuts, pistachios, and areca nuts."
oats,Oats,Oat,Oats,production land_use crop_yield,
offals,Offals,Offal,Offals,food_balances waste animal_feed,
offals_buffaloes,"Offals, buffaloes",Buffalo offal,Buffalo offals,production animals_slaughtered,
offals_camels,"Offals, camels",Camel offal,Camel offals,production animals_slaughtered,
offals_cattle,"Offals, cattle",Cattle offal,Cattle offals,production animals_slaughtered,
offals_goats,"Offals, goats",Goat offal,Goat offals,production animals_slaughtered,
offals_horses,"Offals, horses",Horse offal,Horse offals,production animals_slaughtered,
offals_pigs,"Offals, pigs",Pig offal,Pig offals,production animals_slaughtered,
offals_sheep,"Offals, sheep",Sheep offal,Sheep offals,production animals_slaughtered,
oilcrops,Oilcrops,Oilcrop,Oilcrops,production land_use crop_yield food_balances waste animal_feed,
oilcrops_cake_equivalent,"Oilcrops, cake equivalent","Oilcrop, cake equivalent","Oilcrops, cake equivalent",production land_use crop_yield,"The residues of oilcrops – the crop left after the oil has been extracted – are often used as animal feed for livestock. This component is refered to as 'oilseed cake'."
oilcrops_oil_equivalent,"Oilcrops, oil equivalent","Oilcrop, oil equivalent","Oilcrops, oil equivalent",production land_use crop_yield,
okra,Okra,Okra,Okra,production land_use crop_yield,
olive_oil,Olive oil,Olive oil,Olive oil,production food_balances animal_feed,
olives,Olives,Olive,Olives,production land_use crop_yield food_balances waste,
onions,Onions,Onion,Onions,production land_use crop_yield food_balances waste animal_feed,
oranges,Oranges,Orange,Oranges,production land_use crop_yield food_balances waste animal_feed,
palm_fruit_oil,Palm fruit oil,Palm fruit oil,Palm fruit oil,production land_use crop_yield,
palm_kernel_oil,Palm kernel oil,Palm kernel oil,Palm kernel oil,production food_balances waste,
palm_kernels,Palm kernels,Palm kernel,Palm kernels,production food_balances waste animal_feed,
palm_oil,Palm oil,Palm oil,Palm oil,production,
papayas,Papayas,Papaya,Papayas,production land_use crop_yield,
peaches_and_nectarines,Peaches and nectarines,Peach and nectarine,Peaches and nectarines,production land_use crop_yield,
pears,Pears,Pear,Pears,production land_use crop_yield,
peas_dry,"Peas, dry",Dry pea,Dry peas,production land_use crop_yield food_balances waste animal_feed,
peas_green,"Peas, green",Green pea,Green peas,production land_use crop_yield,
pepper,Pepper,Pepper,Pepper,production land_use crop_yield food_balances waste,
pigeon_peas,Pigeon peas,Pigeon pea,Pigeon peas,production land_use crop_yield,
pineapples,Pineapples,Pineapple,Pineapples,food_balances waste,
pistachios,Pistachios,Pistachio,Pistachios,production land_use crop_yield,
plantains,Plantains,Plantain,Plantains,production land_use crop_yield food_balances waste animal_feed,
plums,Plums,Plum,Plums,production land_use crop_yield,
poppy_seeds,Poppy seed,Poppy seed,Poppy seeds,production land_use crop_yield,
potatoes,Potatoes,Potato,Potatoes,production land_use crop_yield food_balances waste animal_feed,
pulses,Pulses,Pulse,Pulses,production land_use crop_yield food_balances waste animal_feed,"Pulses are the edible seeds of plants in the legume family. The FAO recognizes 11 types of pulses: dry beans, dry broad beans, dry peas, chickpeas, cow peas, pigeon peas, lentils, Bambara beans, vetches, lupins and pulses nes (not elsewhere specified)."
quinoa,Quinoa,Quinoa,Quinoa,production land_use crop_yield,
rapeseed,Rapeseed,Rapeseed,Rapeseeds,production land_use crop_yield,
rapeseed_oil,Rapeseed oil,Rapeseed oil,Rapeseed oil,production,
raspberries,Raspberries,Raspberry,Raspberries,production land_use crop_yield,
rice,Rice,Rice,Rice,production land_use crop_yield food_balances waste animal_feed,
roots_and_tubers,Roots and tubers,Root and tuber,Roots and tubers,production land_use crop_yield,"Roots and tubers is the sum of crops in this category, including cassava, potatoes, sweet potato, yams, and yautia."
rye,Rye,Rye,Rye,production land_use crop_yield food_balances waste animal_feed,
safflower_oil,Safflower oil,Safflower oil,Safflower oil,production,
safflower_seed,Safflower seed,Safflower seed,Safflower seeds,production land_use crop_yield,
seed_cotton,Seed cotton,Seed cotton,Seed cotton,production land_use crop_yield,
sesame_oil,Sesame oil,Sesame oil,Sesame oil,production food_balances,
sesame_seed,Sesame seed,Sesame seed,Sesame seeds,production land_use crop_yield food_balances waste animal_feed,
silk,Silk,Silk,Silk,production,
skins_goat,"Skins, goat",Goat skin,Goat skins,production animals_slaughtered,
skins_sheep,"Skins, sheep",Sheep skin,Sheep skins,production animals_slaughtered,
sorghum,Sorghum,Sorghum,Sorghum,production land_use crop_yield food_balances waste animal_feed,
soybean_oil,Soybean oil,Soybean oil,Soybean oil,production food_balances waste animal_feed,
soybeans,Soybeans,Soybean,Soybeans,production land_use crop_yield food_balances waste animal_feed,
spinach,Spinach,Spinach,Spinach,production land_use crop_yield,
strawberries,Strawberries,Strawberry,Strawberries,production land_use crop_yield,
string_beans,String beans,String bean,String beans,production land_use crop_yield,
sugar_raw,Sugar (raw),Sugar (raw),Sugar (raw),production,"Sugar (raw) is the total quantity of sugar product yielded from sugar cane and sugar beet crops, expressed in its raw equivalents."
sugar_beet,Sugar beet,Sugar beet,Sugar beet,production land_use crop_yield food_balances waste animal_feed,
sugar_cane,Sugar cane,Sugar cane,Sugar cane,production land_use crop_yield food_balances waste animal_feed,
sugar_crops,Sugar crops,Sugar crop,Sugar crops,production land_use crop_yield food_balances waste animal_feed,"Sugar crops is the sum of sugar cane and sugar beet."
sunflower_oil,Sunflower oil,Sunflower oil,Sunflower oil,production food_balances waste animal_feed,
sunflower_seed,Sunflower seed,Sunflower seed,Sunflower seeds,production land_use crop_yield food_balances waste animal_feed,
sweet_potatoes,Sweet potatoes,Sweet potato,Sweet potatoes,production land_use crop_yield food_balances waste animal_feed,
tangerines,Tangerines,Tangerine,Tangerines,production land_use crop_yield,
tea,Tea,Tea,Tea,production land_use crop_yield,
tobacco,Tobacco,Tobacco,Tobacco,production land_use crop_yield,
tomatoes,Tomatoes,Tomato,Tomatoes,production land_use crop_yield food_balances waste animal_feed,
total,Total,All food,All foods,total_food_supply,"This is the total of all agricultural produce – both crops and livestock."
treenuts,Treenuts,Treenut,Treenuts,production land_use crop_yield,
vegetables,Vegetables,Vegetable,Vegetables,production land_use crop_yield food_balances waste animal_feed,
walnuts,Walnuts,Walnut,Walnuts,production land_use crop_yield,
watermelons,Watermelons,Watermelon,Watermelons,production land_use crop_yield,
wheat,Wheat,Wheat,Wheat,production land_use crop_yield food_balances waste animal_feed,
whey,Whey,Whey,Whey,production,
wine,Wine,Wine,Wine,food_balances waste,
wool,Wool,Wool,Wool,production,
yams,Yams,Yam,Yams,production land_use crop_yield food_balances waste animal_feed,
"""

VIEWS_PER_FOOD_CONTENT = """_tags,title,subtitle,Metric Dropdown,Unit Radio,Per Capita Checkbox,type,ySlugs,yScaleToggle,note
production,${food_singular} production,,Production,,false,LineChart,production__tonnes,true,
production,Per capita ${food_singular_lower} production,,Production,,true,LineChart,production__kg__per_capita,true,
animals_slaughtered,Animals slaughtered to produce ${food_plural_lower},,Animals slaughtered,,false,LineChart,producing_or_slaughtered_animals__animals,true,
animals_slaughtered,Animals slaughtered per capita to produce ${food_plural_lower},,Animals slaughtered,,true,LineChart,producing_or_slaughtered_animals__animals__per_capita,true,
crop_yield,${food_singular} yield,Yield is measured as the quantity produced per unit area of land used to grow it.,Yield,,false,LineChart,yield__tonnes_per_ha,true,
animal_yield,${food_singular} yield,Yield is measured as the quantity produced per animal.,Yield,,false,LineChart,yield__kg_per_animal,true,
land_use,Land used for ${food_singular_lower} production,The amount of cropland used for production.,Land Use,,false,LineChart,area_harvested__ha,true,
land_use,Land used for ${food_singular_lower} production per capita,The amount of cropland used for production.,Land Use,,true,LineChart,area_harvested__m2__per_capita,true,
food_balances,Per capita ${food_singular_lower} supply per year,"This measures the quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower than this value.",Food available for consumption,Kilograms per year,true,LineChart,food_available_for_consumption__kg_per_year__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,Per capita ${food_singular_lower} supply per day,"This measures the quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower than this value.",Food available for consumption,Grams per day,true,LineChart,food_available_for_consumption__g_per_day__per_capita,true,The FAO apply a methodological change from the year 2010 onwards
food_balances total_food_supply,Per capita kilocalorie supply from ${food_plural_lower} per day,"This measures the quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower than this value.",Food available for consumption,Kilocalories per day,true,LineChart,food_available_for_consumption__kcal_per_day__per_capita,true,The FAO apply a methodological change from the year 2010 onwards
food_balances total_food_supply,Per capita protein supply from ${food_plural_lower} per day,"This measures the quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower than this value.",Food available for consumption,Protein per day,true,LineChart,food_available_for_consumption__protein_g_per_day__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances total_food_supply,Per capita fat supply from ${food_plural_lower} per day,"This measures the quantity that is available for consumption at the end of the supply chain. It does not account for consumer waste, so the quantity that is actually consumed may be lower than this value.",Food available for consumption,Fat per day,true,LineChart,food_available_for_consumption__fat_g_per_day__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,${food_singular} imports,The quantity that is imported in a given year.,Imports,,false,LineChart,imports__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,Per capita ${food_singular_lower} imports,The quantity that is imported in a given year.,Imports,,true,LineChart,imports__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,${food_singular} exports,The quantity that is exported in a given year.,Exports,,false,LineChart,exports__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,Per capita ${food_singular_lower} exports,The quantity that is exported in a given year.,Exports,,true,LineChart,exports__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,Domestic supply of ${food_plural_lower},"This measures the supply that is available after trade. It is calculated as production, plus imports, minus exports.",Domestic supply (after trade),,false,LineChart,domestic_supply__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,Per capita domestic supply of ${food_plural_lower},"This measures the supply that is available after trade. It is calculated as production, plus imports, minus exports.",Domestic supply (after trade),,true,LineChart,domestic_supply__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
waste,${food_singular} waste in supply chains,"The quantity that is lost or wasted in supply chains through poor handling, spoiling, lack of refrigeration and damage from the field to retail. It does not include consumer waste.",Waste in supply chains,,false,LineChart,waste_in_supply_chain__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
waste,Per capita ${food_singular_lower} waste in supply chains,"The quantity that is lost or wasted in supply chains through poor handling, spoiling, lack of refrigeration and damage from the field to retail. It does not include consumer waste.",Waste in supply chains,,true,LineChart,waste_in_supply_chain__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,${food_plural} used for direct human food,"The quantity that is allocated for direct consumption as human food, rather than allocation to animal feed or industrial uses.",Allocated for human food,,false,LineChart,food__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,"${food_plural} used for direct human food, per capita","The quantity that is allocated for direct consumption as human food, rather than allocation to animal feed or industrial uses.",Allocated for human food,,true,LineChart,food__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,${food_plural} used for industrial uses,"The quantity that is allocated to industrial uses such as biofuel, pharmaceuticals or textile products.",Allocated to industrial uses,,false,LineChart,other_uses__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
food_balances,"${food_plural} used for industrial uses, per capita","The quantity that is allocated to industrial uses such as biofuel, pharmaceuticals or textile products.",Allocated to industrial uses,,true,LineChart,other_uses__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
animal_feed,${food_plural} used for animal feed,The quantity that is allocated to feed for livestock.,Allocated to animal feed,,false,LineChart,feed__tonnes,true,The FAO apply a methodological change from the year 2010 onwards.
animal_feed,"${food_plural} used for animal feed, per capita",The quantity that is allocated to feed for livestock.,Allocated to animal feed,,true,LineChart,feed__kg__per_capita,true,The FAO apply a methodological change from the year 2010 onwards.
"""


def run():
    #
    # Load inputs.
    #
    # Load the template for the explorer tsv file.
    template = Template(TEMPLATE_CONTENT)
    # Load the foods data.
    foods_df = pd.read_csv(StringIO(FOODS_CONTENT), index_col="slug", dtype=str)
    # Load the views per food data.
    views_df = pd.read_csv(StringIO(VIEWS_PER_FOOD_CONTENT), dtype=str)

    # Determine whether the script is executed on staging or production.
    if OWID_ENV.name == "production":
        DATA_FILES_URL = "https://catalog.ourworldindata.org/explorers/faostat/latest/food_explorer/"
    else:
        DATA_FILES_URL = f"http://{OWID_ENV.name}:8881/explorers/faostat/latest/food_explorer/"

    log.info(f"Creating csv-based explorer that will read files like, e.g. {DATA_FILES_URL + 'almonds.csv'}")

    #
    # Process data.
    #
    def table_def(food):
        return f"table\t{DATA_FILES_URL}{food}.csv\t{food}"

    # convert space-separated list of tags to an actual list, such that we can explode and merge by tag
    views_df["_tags"] = views_df["_tags"].apply(lambda x: x.split(" "))
    views_df = views_df.explode("_tags").rename(columns={"_tags": "_tag"})
    views_df["_tag"] = views_df["_tag"].str.strip()

    foods_rename = {
        "dropdown": "Food Dropdown",
        "slug": "tableSlug",
        "_tags": "_tags",
        "note": "food__note",
    }

    foods = foods_df.reset_index()[foods_rename.keys()].rename(columns=foods_rename)
    foods["_tags"] = foods["_tags"].apply(lambda x: x.split(" "))
    foods = foods.explode("_tags").rename(columns={"_tags": "_tag"})

    food_tags = set(foods["_tag"])
    view_tags = set(views_df["_tag"])

    symmetric_diff = food_tags.symmetric_difference(view_tags)
    if len(symmetric_diff) > 0:
        log.error(
            f"Found {len(symmetric_diff)} tags that only appear in one of the input files: {', '.join(symmetric_diff)}"
        )

    def substitute_title(row):
        # The title can include placeholders like ${food_singular}, which will be replaced with the actual food name here.
        food_slug = row["tableSlug"]
        food_names = foods_df.loc[food_slug]
        for key in ["title", "subtitle"]:
            if isinstance(row[key], str):
                template = Template(row[key])
                row[key] = template.substitute(
                    food_singular=food_names["singular"],
                    food_singular_lower=food_names["singular"].lower(),
                    food_plural=food_names["plural"],
                    food_plural_lower=food_names["plural"].lower(),
                )
        return row

    # merge on column: _tag
    graphers = views_df.merge(foods).apply(substitute_title, axis=1)
    graphers = graphers.drop(columns="_tag").sort_values(by="Food Dropdown", kind="stable")  # type: ignore
    # drop duplicates introduced by the tag merge
    graphers = graphers.drop_duplicates()

    # join note (footnote) between food and view tables
    graphers["note"] = graphers["food__note"].str.cat(graphers["note"], sep="\\n", na_rep="")
    graphers["note"] = graphers["note"].apply(lambda x: x.strip("\\n"))
    graphers = graphers.drop(columns="food__note")

    # We want to have a consistent column order for easier interpretation of the output.
    # However, if there are any columns added to views-per-food.csv at any point in the future,
    # we want to make sure these are also present in the output.
    # Therefore, we define the column order and also add any remaining columns to the output.
    col_order = [
        "title",
        "Food Dropdown",
        "Metric Dropdown",
        "Unit Radio",
        "Per Capita Checkbox",
        "subtitle",
        "type",
        "ySlugs",
        "tableSlug",
        "note",
        "yScaleToggle",
    ]
    remaining_cols = pd.Index(graphers.columns).difference(pd.Index(col_order)).tolist()
    graphers = graphers.reindex(columns=col_order + remaining_cols)

    if len(remaining_cols) > 0:
        log.warning("ℹ️ Found the following columns not present in col_order:", remaining_cols)

    # Define the default view of the explorer.
    default_view = (
        '`Food Dropdown` == "Maize (corn)" and `Metric Dropdown` == "Production" and `Per Capita Checkbox` == "false"'
    )

    # Mark the default view with defaultView=true. This is always the last column.
    if default_view is not None:
        default_view_mask = graphers.eval(default_view)
        default_view_count = len(graphers[default_view_mask])
        if default_view_count != 1:
            log.error(
                f"Default view ({default_view}) should match exactly one view, but matches {default_view_count} views: {graphers[default_view_mask]}"
            )
        graphers["defaultView"] = np.where(default_view_mask, "true", None)  # type: ignore

    # Prepare graphers table.
    graphers_tsv = graphers.to_csv(sep="\t", index=False)
    graphers_tsv_indented = textwrap.indent(graphers_tsv, "\t")  # type: ignore

    table_defs = "\n".join([table_def(food) for food in foods_df.index])
    food_slugs = "\t".join(foods_df.index)

    # Get the latest publication year of the data used in the food explorer.
    # To do that, get the most recent publication year of the fbsc and qcl datasets (the two datasets used by the explorer).
    YEAR = find("faostat_qcl|fbsc").sort_values("version", ascending=False).iloc[0]["version"].split("-")[0]

    # Generate the content of the explorer.tsv file.
    warning_message = "# DO NOT EDIT THIS FILE BY HAND. It is automatically generated using a set of input files. Any changes made directly to it will be overwritten.\n\n"
    explorer_content = warning_message + template.substitute(
        food_slugs=food_slugs,
        graphers_tsv=graphers_tsv_indented,
        table_defs=table_defs,
        year=YEAR,
    )

    #
    # Save outputs.
    #
    # DEBUGGING: Write the explorer.tsv file in a local file, to visually inspect it before pushing to DB.
    # Write explorer.tsv file in the current folder (temporarily).
    # with open("./global-food.explorer.tsv", "w", newline="\n") as f:
    #     f.write(explorer_content)

    # Write explorer tsv content to DB.
    admin_api = AdminAPI(OWID_ENV)
    admin_api.put_explorer_config("global-food", explorer_content)
