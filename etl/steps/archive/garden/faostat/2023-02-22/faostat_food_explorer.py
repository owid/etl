"""Dataset feeding the global food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food items (now called
products).

The resulting dataset will later be loaded by the `explorer/food_explorer` which feeds our
[Global food explorer](https://ourworldindata.org/explorers/global-food).

"""

from pathlib import Path
from typing import cast

import pandas as pd
from owid import catalog
from owid.datautils import dataframes
from shared import CURRENT_DIR, NAMESPACE

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Dataset name and title.
DATASET_TITLE = "Food Explorer"
DATASET_DESCRIPTION = (
    "This dataset has been created by Our World in Data, merging existing FAOSTAT datasets. In "
    "particular, we have used 'Crops and livestock products' (QCL) and 'Food Balances' (FBSH and "
    "FBS) datasets. Each row contains all the metrics for a specific combination of (country, "
    "product, year). The metrics may come from different datasets."
)

# The names of the products to include in the food explorer will be further edited in owid-content, following to the
# following file:
# https://github.com/owid/owid-content/blob/master/scripts/global-food-explorer/foods.csv
ITEM_CODES_QCL = [
    "00000060",  # From faostat_qcl - 'Maize oil' (previously 'Maize oil').
    "00000567",  # From faostat_qcl - 'Watermelons' (previously 'Watermelons').
    "00000075",  # From faostat_qcl - 'Oats' (previously 'Oats').
    "00000191",  # From faostat_qcl - 'Chickpeas' (previously 'Chickpeas').
    "00001069",  # From faostat_qcl - 'Meat of ducks, fresh or chilled' (previously 'Meat, duck').
    "00000957",  # From faostat_qcl - 'Buffalo hides' (previously 'Buffalo hides').
    "00000176",  # From faostat_qcl - 'Beans, dry' (previously 'Beans, dry').
    "00001182",  # From faostat_qcl - 'Honey' (previously 'Honey').
    "00000399",  # From faostat_qcl - 'Eggplants' (previously 'Eggplants').
    "00000554",  # From faostat_qcl - 'Cranberries' (previously 'Cranberries').
    "00000296",  # From faostat_qcl - 'Poppy seeds' (previously 'Poppy seeds').
    "00000201",  # From faostat_qcl - 'Lentils, dry' (previously 'Lentils').
    "00000268",  # From faostat_qcl - 'Sunflower oil' (previously 'Sunflower oil').
    "00001806",  # From faostat_qcl - 'Meat, beef and buffalo' (previously 'Meat, beef and buffalo').
    "00000600",  # From faostat_qcl - 'Papayas' (previously 'Papayas').
    "00000334",  # From faostat_qcl - 'Linseed oil' (previously 'Linseed oil').
    "00001097",  # From faostat_qcl - 'Horse meat, fresh or chilled' (previously 'Meat, horse').
    "00000165",  # From faostat_qcl - 'Molasses' (previously 'Molasses').
    "00000426",  # From faostat_qcl - 'Carrots and turnips' (previously 'Carrots and turnips').
    "00000216",  # From faostat_qcl - 'Brazil nuts, in shell' (previously 'Brazil nuts, with shell').
    "00000137",  # From faostat_qcl - 'Yams' (previously 'Yams').
    "00000222",  # From faostat_qcl - 'Walnuts' (previously 'Walnuts').
    "00000289",  # From faostat_qcl - 'Sesame seed' (previously 'Sesame seed').
    "00000122",  # From faostat_qcl - 'Sweet potatoes' (previously 'Sweet potatoes').
    "00001738",  # From faostat_qcl - 'Fruit' (previously 'Fruit').
    "00001780",  # From faostat_qcl - 'Milk' (previously 'Milk').
    "00001804",  # From faostat_qcl - 'Citrus Fruit' (previously 'Citrus Fruit').
    "00000656",  # From faostat_qcl - 'Coffee, green' (previously 'Coffee, green').
    "00001019",  # From faostat_qcl - 'Goat fat, unrendered' (previously 'Fat, goats').
    "00000225",  # From faostat_qcl - 'Hazelnuts' (previously 'Hazelnuts').
    "00000406",  # From faostat_qcl - 'Green garlic' (previously 'Garlic').
    "00000995",  # From faostat_qcl - 'Skins, sheep' (previously 'Skins, sheep').
    "00000244",  # From faostat_qcl - 'Groundnut oil' (previously 'Groundnut oil').
    "00000281",  # From faostat_qcl - 'Safflower oil' (previously 'Safflower oil').
    "00000267",  # From faostat_qcl - 'Sunflower seed' (previously 'Sunflower seed').
    "00001025",  # From faostat_qcl - 'Skins, goat' (previously 'Skins, goat').
    "00000252",  # From faostat_qcl - 'Coconut oil' (previously 'Coconut oil').
    "00000256",  # From faostat_qcl - 'Palm kernels' (previously 'Palm kernels').
    "00000868",  # From faostat_qcl - 'Offals, cattle' (previously 'Offals, cattle').
    "00000292",  # From faostat_qcl - 'Mustard seed' (previously 'Mustard seed').
    "00000101",  # From faostat_qcl - 'Canary seed' (previously 'Canary seed').
    "00001098",  # From faostat_qcl - 'Edible offals of horses and other equines,  fresh, chilled or frozen' (previously 'Offals, horses').
    "00001062",  # From faostat_qcl - 'Eggs from hens' (previously 'Eggs from hens').
    "00001808",  # From faostat_qcl - 'Meat, poultry' (previously 'Meat, poultry').
    "00000258",  # From faostat_qcl - 'Palm kernel oil' (previously 'Palm kernel oil').
    "00000156",  # From faostat_qcl - 'Sugar cane' (previously 'Sugar cane').
    "00000373",  # From faostat_qcl - 'Spinach' (previously 'Spinach').
    "00000773",  # From faostat_qcl - 'Flax fibre' (previously 'Flax fibre').
    "00000116",  # From faostat_qcl - 'Potatoes' (previously 'Potatoes').
    "00000869",  # From faostat_qcl - 'Cattle fat, unrendered' (previously 'Fat, cattle').
    "00000358",  # From faostat_qcl - 'Cabbages' (previously 'Cabbages').
    "00000767",  # From faostat_qcl - 'Cotton' (previously 'Cotton').
    "00000388",  # From faostat_qcl - 'Tomatoes' (previously 'Tomatoes').
    "00000220",  # From faostat_qcl - 'Chestnuts, in shell' (previously 'Chestnut').
    "00000027",  # From faostat_qcl - 'Rice' (previously 'Rice').
    "00000367",  # From faostat_qcl - 'Asparagus' (previously 'Asparagus').
    "00000977",  # From faostat_qcl - 'Meat, lamb and mutton' (previously 'Meat, lamb and mutton').
    "00000015",  # From faostat_qcl - 'Wheat' (previously 'Wheat').
    "00001127",  # From faostat_qcl - 'Meat of camels, fresh or chilled' (previously 'Meat, camel').
    "00001183",  # From faostat_qcl - 'Beeswax' (previously 'Beeswax').
    "00001720",  # From faostat_qcl - 'Roots and tubers' (previously 'Roots and tubers').
    "00001186",  # From faostat_qcl - 'Silk' (previously 'Silk').
    "00000826",  # From faostat_qcl - 'Tobacco' (previously 'Tobacco').
    "00000978",  # From faostat_qcl - 'Offals, sheep' (previously 'Offals, sheep').
    "00000948",  # From faostat_qcl - 'Offals, buffaloes' (previously 'Offals, buffaloes').
    "00000226",  # From faostat_qcl - 'Areca nuts' (previously 'Areca nuts').
    "00000417",  # From faostat_qcl - 'Peas, green' (previously 'Peas, green').
    "00000407",  # From faostat_qcl - 'Leeks' (previously 'Leeks').
    "00000224",  # From faostat_qcl - 'Kola nuts' (previously 'Kola nuts').
    "00000079",  # From faostat_qcl - 'Millet' (previously 'Millet').
    "00000568",  # From faostat_qcl - 'Melon' (previously 'Melon').
    "00000900",  # From faostat_qcl - 'Whey' (previously 'Whey').
    "00000544",  # From faostat_qcl - 'Strawberries' (previously 'Strawberries').
    "00000333",  # From faostat_qcl - 'Linseed' (previously 'Linseed').
    "00000571",  # From faostat_qcl - 'Mangoes' (previously 'Mangoes').
    "00000534",  # From faostat_qcl - 'Peaches and nectarines' (previously 'Peaches and nectarines').
    "00000372",  # From faostat_qcl - 'Lettuce' (previously 'Lettuce').
    "00001080",  # From faostat_qcl - 'Meat of turkeys, fresh or chilled' (previously 'Meat, turkey').
    "00000083",  # From faostat_qcl - 'Sorghum' (previously 'Sorghum').
    "00001732",  # From faostat_qcl - 'Oilcrops, Oil Equivalent' (previously 'Oilcrops, Oil Equivalent').
    "00000336",  # From faostat_qcl - 'Hempseed' (previously 'Hempseed').
    "00000397",  # From faostat_qcl - 'Cucumbers and gherkins' (previously 'Cucumbers and gherkins').
    "00000223",  # From faostat_qcl - 'Pistachios, in shell' (previously 'Pistachios').
    "00000242",  # From faostat_qcl - 'Groundnuts' (previously 'Groundnuts').
    "00000489",  # From faostat_qcl - 'Plantains' (previously 'Plantains').
    "00000495",  # From faostat_qcl - 'Tangerines' (previously 'Tangerines').
    "00000195",  # From faostat_qcl - 'Cow peas' (previously 'Cow peas').
    "00000290",  # From faostat_qcl - 'Sesame oil' (previously 'Sesame oil').
    "00000497",  # From faostat_qcl - 'Lemons and limes' (previously 'Lemons and limes').
    "00000711",  # From faostat_qcl - 'Herbs (e.g. fennel)' (previously 'Herbs (e.g. fennel)').
    "00001129",  # From faostat_qcl - 'Fat of camels' (previously 'Fat, camels').
    "00000577",  # From faostat_qcl - 'Dates' (previously 'Dates').
    "00001108",  # From faostat_qcl - 'Meat of asses, fresh or chilled' (previously 'Meat, ass').
    "00000071",  # From faostat_qcl - 'Rye' (previously 'Rye').
    "00001073",  # From faostat_qcl - 'Meat of geese, fresh or chilled' (previously 'Meat, goose and guinea fowl').
    "00000687",  # From faostat_qcl - 'Pepper' (previously 'Pepper').
    "00000280",  # From faostat_qcl - 'Safflower seed' (previously 'Safflower seed').
    "00000157",  # From faostat_qcl - 'Sugar beet' (previously 'Sugar beet').
    "00000271",  # From faostat_qcl - 'Rapeseed oil' (previously 'Rapeseed oil').
    "00001735",  # From faostat_qcl - 'Vegetables' (previously 'Vegetables').
    "00001035",  # From faostat_qcl - 'Meat of pig with the bone, fresh or chilled' (previously 'Meat, pig').
    "00001128",  # From faostat_qcl - 'Offals, camels' (previously 'Offals, camels').
    "00000564",  # From faostat_qcl - 'Wine' (previously 'Wine').
    "00000092",  # From faostat_qcl - 'Quinoa' (previously 'Quinoa').
    "00000507",  # From faostat_qcl - 'Grapefruit' (previously 'Grapefruit').
    "00000089",  # From faostat_qcl - 'Buckwheat' (previously 'Buckwheat').
    "00000949",  # From faostat_qcl - 'Buffalo fat, unrendered' (previously 'Fat, buffaloes').
    "00000821",  # From faostat_qcl - 'Fibre crops' (previously 'Fibre crops').
    "00000221",  # From faostat_qcl - 'Almonds' (previously 'Almonds').
    "00000328",  # From faostat_qcl - 'Seed cotton, unginned' (previously 'Seed cotton').
    "00001717",  # From faostat_qcl - 'Cereals' (previously 'Cereals').
    "00000547",  # From faostat_qcl - 'Raspberries' (previously 'Raspberries').
    "00000187",  # From faostat_qcl - 'Peas, dry' (previously 'Peas, dry').
    "00000560",  # From faostat_qcl - 'Grapes' (previously 'Grapes').
    "00000689",  # From faostat_qcl - 'Chillies and peppers' (previously 'Chillies and peppers').
    "00001091",  # From faostat_qcl - 'Eggs from other birds (excl. hens)' (previously 'Eggs from other birds (excl. hens)').
    "00001163",  # From faostat_qcl - 'Game meat, fresh, chilled or frozen' (previously 'Meat, game').
    "00001807",  # From faostat_qcl - 'Meat, sheep and goat' (previously 'Meat, sheep and goat').
    "00001141",  # From faostat_qcl - 'Meat of rabbits and hares, fresh or chilled' (previously 'Meat, rabbit').
    "00000490",  # From faostat_qcl - 'Oranges' (previously 'Oranges').
    "00001841",  # From faostat_qcl - 'Oilcrops, Cake Equivalent' (previously 'Oilcrops, Cake Equivalent').
    "00000552",  # From faostat_qcl - 'Blueberries' (previously 'Blueberries').
    "00001783",  # From faostat_qcl - 'Eggs' (previously 'Eggs').
    "00000254",  # From faostat_qcl - 'Palm fruit oil' (previously 'Palm fruit oil').
    "00000263",  # From faostat_qcl - 'Karite nuts' (previously 'Karite nuts').
    "00000044",  # From faostat_qcl - 'Barley' (previously 'Barley').
    "00001036",  # From faostat_qcl - 'Offals, pigs' (previously 'Offals, pigs').
    "00000446",  # From faostat_qcl - 'Green maize' (previously 'Green maize').
    "00001745",  # From faostat_qcl - 'Cheese' (previously 'Cheese').
    "00000261",  # From faostat_qcl - 'Olive oil' (previously 'Olive oil').
    "00000236",  # From faostat_qcl - 'Soya beans' (previously 'Soybeans').
    "00000125",  # From faostat_qcl - 'Cassava, fresh' (previously 'Cassava').
    "00000260",  # From faostat_qcl - 'Olives' (previously 'Olives').
    "00000329",  # From faostat_qcl - 'Cotton seed' (previously 'Cottonseed').
    "00000521",  # From faostat_qcl - 'Pears' (previously 'Pears').
    "00001018",  # From faostat_qcl - 'Offals, goats' (previously 'Offals, goats').
    "00001765",  # From faostat_qcl - 'Meat, total' (previously 'Meat, total').
    "00000550",  # From faostat_qcl - 'Currants' (previously 'Currants').
    "00001058",  # From faostat_qcl - 'Meat of chickens, fresh or chilled' (previously 'Meat, chicken').
    "00000197",  # From faostat_qcl - 'Pigeon peas, dry' (previously 'Pigeon peas').
    "00000270",  # From faostat_qcl - 'Rape or colza seed' (previously 'Rapeseed').
    "00000526",  # From faostat_qcl - 'Apricots' (previously 'Apricots').
    "00000592",  # From faostat_qcl - 'Kiwi' (previously 'Kiwi').
    "00000237",  # From faostat_qcl - 'Soybean oil' (previously 'Soybean oil').
    "00000947",  # From faostat_qcl - 'Meat of buffalo, fresh or chilled' (previously 'Meat, buffalo').
    "00000265",  # From faostat_qcl - 'Castor oil seeds' (previously 'Castor oil seed').
    "00000430",  # From faostat_qcl - 'Okra' (previously 'Okra').
    "00000331",  # From faostat_qcl - 'Cottonseed oil' (previously 'Cottonseed oil').
    "00000103",  # From faostat_qcl - 'Mixed grains' (previously 'Mixed grains').
    "00000486",  # From faostat_qcl - 'Bananas' (previously 'Bananas').
    "00000919",  # From faostat_qcl - 'Cattle hides' (previously 'Cattle hides').
    "00001242",  # From faostat_qcl - 'Margarine' (previously 'Margarine').
    "00000449",  # From faostat_qcl - 'Mushrooms' (previously 'Mushrooms').
    "00001037",  # From faostat_qcl - 'Fat of pigs' (previously 'Fat, pigs').
    "00001729",  # From faostat_qcl - 'Treenuts' (previously 'Treenuts').
    "00000366",  # From faostat_qcl - 'Artichokes' (previously 'Artichokes').
    "00000217",  # From faostat_qcl - 'Cashew nuts' (previously 'Cashew nuts').
    "00000299",  # From faostat_qcl - 'Melonseed' (previously 'Melonseed').
    "00000574",  # From faostat_qcl - 'Pineapples' (previously 'Pineapples').
    "00000979",  # From faostat_qcl - 'Sheep fat, unrendered' (previously 'Fat, sheep').
    "00000987",  # From faostat_qcl - 'Wool' (previously 'Wool').
    "00000423",  # From faostat_qcl - 'String beans' (previously 'String beans').
    "00000249",  # From faostat_qcl - 'Coconuts, in shell' (previously 'Coconuts').
    "00000780",  # From faostat_qcl - 'Jute, raw or retted' (previously 'Jute').
    "00000536",  # From faostat_qcl - 'Plums' (previously 'Plums').
    "00001111",  # From faostat_qcl - 'Meat of mules, fresh or chilled' (previously 'Meat, mule').
    "00001723",  # From faostat_qcl - 'Sugar crops' (previously 'Sugar crops').
    "00001726",  # From faostat_qcl - 'Pulses' (previously 'Pulses').
    "00000162",  # From faostat_qcl - 'Sugar (raw)' (previously 'Sugar (raw)').
    "00000667",  # From faostat_qcl - 'Tea leaves' (previously 'Tea').
    "00000056",  # From faostat_qcl - 'Maize (corn)' (previously 'Maize').
    "00000257",  # From faostat_qcl - 'Palm oil' (previously 'Palm oil').
    "00000393",  # From faostat_qcl - 'Cauliflowers and broccoli' (previously 'Cauliflowers and broccoli').
    "00000531",  # From faostat_qcl - 'Cherries' (previously 'Cherries').
    "00000572",  # From faostat_qcl - 'Avocados' (previously 'Avocados').
    "00000403",  # From faostat_qcl - 'Onions' (previously 'Onions').
    "00000515",  # From faostat_qcl - 'Apples' (previously 'Apples').
    "00000414",  # From faostat_qcl - 'Other beans, green' (previously 'Beans, green').
    "00001017",  # From faostat_qcl - 'Meat of goat, fresh or chilled' (previously 'Meat, goat').
    "00000181",  # From faostat_qcl - 'Broad beans' (previously 'Broad beans').
]

ITEM_CODES_FBSC = [
    "00002576",  # From faostat_fbsc - 'Palm kernel oil' (previously 'Palm kernel oil').
    "00002516",  # From faostat_fbsc - 'Oats' (previously 'Oats').
    "00002562",  # From faostat_fbsc - 'Palm kernels' (previously 'Palm kernels').
    "00002551",  # From faostat_fbsc - 'Nuts' (previously 'Nuts').
    "00002913",  # From faostat_fbsc - 'Oilcrops' (previously 'Oilcrops').
    "00002533",  # From faostat_fbsc - 'Sweet potatoes' (previously 'Sweet potatoes').
    "00002560",  # From faostat_fbsc - 'Coconuts' (previously 'Coconuts').
    "00002511",  # From faostat_fbsc - 'Wheat' (previously 'Wheat').
    "00002557",  # From faostat_fbsc - 'Sunflower seed' (previously 'Sunflower seed').
    "00002602",  # From faostat_fbsc - 'Onions' (previously 'Onions').
    "00002734",  # From faostat_fbsc - 'Meat, poultry' (previously 'Meat, poultry').
    "00002572",  # From faostat_fbsc - 'Groundnut oil' (previously 'Groundnut oil').
    "00002736",  # From faostat_fbsc - 'Offals' (previously 'Offals').
    "00002579",  # From faostat_fbsc - 'Sesame oil' (previously 'Sesame oil').
    "00002552",  # From faostat_fbsc - 'Groundnuts' (previously 'Groundnuts').
    "00002943",  # From faostat_fbsc - 'Meat, total' (previously 'Meat, total').
    "00002912",  # From faostat_fbsc - 'Treenuts' (previously 'Treenuts').
    "00002611",  # From faostat_fbsc - 'Oranges' (previously 'Oranges').
    "00002616",  # From faostat_fbsc - 'Plantains' (previously 'Plantains').
    "00002617",  # From faostat_fbsc - 'Apples' (previously 'Apples').
    "00002563",  # From faostat_fbsc - 'Olives' (previously 'Olives').
    "00002513",  # From faostat_fbsc - 'Barley' (previously 'Barley').
    "00002532",  # From faostat_fbsc - 'Cassava' (previously 'Cassava').
    "00002918",  # From faostat_fbsc - 'Vegetables' (previously 'Vegetables').
    "00002948",  # From faostat_fbsc - 'Milk' (previously 'Milk').
    "00002613",  # From faostat_fbsc - 'Grapefruit' (previously 'Grapefruit').
    "00002555",  # From faostat_fbsc - 'Soybeans' (previously 'Soybeans').
    "00002537",  # From faostat_fbsc - 'Sugar beet' (previously 'Sugar beet').
    "00002640",  # From faostat_fbsc - 'Pepper' (previously 'Pepper').
    "00002536",  # From faostat_fbsc - 'Sugar cane' (previously 'Sugar cane').
    "00002633",  # From faostat_fbsc - 'Cocoa beans' (previously 'Cocoa beans').
    "00002561",  # From faostat_fbsc - 'Sesame seed' (previously 'Sesame seed').
    "00002546",  # From faostat_fbsc - 'Beans, dry' (previously 'Beans, dry').
    "00002740",  # From faostat_fbsc - 'Butter and ghee' (previously 'Butter and ghee').
    "00002514",  # From faostat_fbsc - 'Maize' (previously 'Maize').
    "00002575",  # From faostat_fbsc - 'Cottonseed oil' (previously 'Cottonseed oil').
    "00002641",  # From faostat_fbsc - 'Chillies and peppers' (previously 'Chillies and peppers').
    "00002733",  # From faostat_fbsc - 'Pork' (previously 'Pork').
    "00002919",  # From faostat_fbsc - 'Fruit' (previously 'Fruit').
    "00002655",  # From faostat_fbsc - 'Wine' (previously 'Wine').
    "00002618",  # From faostat_fbsc - 'Pineapples' (previously 'Pineapples').
    "00002612",  # From faostat_fbsc - 'Lemons and limes' (previously 'Lemons and limes').
    "00002580",  # From faostat_fbsc - 'Olive oil' (previously 'Olive oil').
    "00002515",  # From faostat_fbsc - 'Rye' (previously 'Rye').
    "00002582",  # From faostat_fbsc - 'Maize oil' (previously 'Maize oil').
    "00002731",  # From faostat_fbsc - 'Meat, beef' (previously 'Meat, beef').
    "00002518",  # From faostat_fbsc - 'Sorghum' (previously 'Sorghum').
    "00002949",  # From faostat_fbsc - 'Eggs' (previously 'Eggs').
    "00002531",  # From faostat_fbsc - 'Potatoes' (previously 'Potatoes').
    "00002615",  # From faostat_fbsc - 'Bananas' (previously 'Bananas').
    "00002573",  # From faostat_fbsc - 'Sunflower oil' (previously 'Sunflower oil').
    "00002578",  # From faostat_fbsc - 'Coconut oil' (previously 'Coconut oil').
    "00002601",  # From faostat_fbsc - 'Tomatoes' (previously 'Tomatoes').
    "00002571",  # From faostat_fbsc - 'Soybean oil' (previously 'Soybean oil').
    "00002559",  # From faostat_fbsc - 'Cottonseed' (previously 'Cottonseed').
    "00002732",  # From faostat_fbsc - 'Meat, sheep and goat' (previously 'Meat, sheep and goat').
    "00002901",  # From faostat_fbsc - 'Total' (previously 'Total').
    "00002619",  # From faostat_fbsc - 'Dates' (previously 'Dates').
    "00002911",  # From faostat_fbsc - 'Pulses' (previously 'Pulses').
    "00002535",  # From faostat_fbsc - 'Yams' (previously 'Yams').
    "00002745",  # From faostat_fbsc - 'Honey' (previously 'Honey').
    "00002737",  # From faostat_fbsc - 'Animal fats' (previously 'Animal fats').
    "00002517",  # From faostat_fbsc - 'Millet' (previously 'Millet').
    "00002547",  # From faostat_fbsc - 'Peas, dry' (previously 'Peas, dry').
    "00002807",  # From faostat_fbsc - 'Rice' (previously 'Rice').
    "00002960",  # From faostat_fbsc - 'Fish and seafood' (previously 'Fish and seafood').
    "00002908",  # From faostat_fbsc - 'Sugar crops' (previously 'Sugar crops').
]

# OWID item name, element name, and unit name for population (as given in faostat_qcl and faostat_fbsc datasets).
FAO_POPULATION_ITEM_NAME = "Population"
FAO_POPULATION_ELEMENT_NAME = "Total Population - Both sexes"
FAO_POPULATION_UNIT = "1000 persons"

# List of element codes to consider from faostat_qcl.
ELEMENT_CODES_QCL = [
    "005312",
    "005313",
    "005314",
    "005318",
    "005320",
    "005321",
    "005410",
    "005413",
    "005417",
    "005419",
    "005420",
    "005422",
    "005424",
    "005510",
    "005513",
    "5312pc",
    "5320pc",
    "5321pc",
    "5510pc",
]
# List of element codes to consider from faostat_fbsc.
ELEMENT_CODES_FBSC = [
    "000645",
    "000664",
    "000674",
    "000684",
    "005072",
    "005123",
    "005131",
    "005142",
    "005154",
    "005170",
    "005171",
    "005301",
    # Element 'Production' (in tonnes, originally given in 1000 tonnes) is taken from qcl.
    # Although fbsc has items for this element that are not in qcl, they overlap in a number of items with slightly
    # different values. To avoid this issue, we ignore the element from fbsc and use only the one in qcl.
    # '005511',
    "005521",
    "005527",
    "005611",
    "005911",
    "0645pc",
    "0664pc",
    "0674pc",
    "0684pc",
    "5123pc",
    "5142pc",
    "5154pc",
    "5301pc",
    "5521pc",
    "5611pc",
    "5911pc",
    # The following element code is for population.
    "000511",
]


def combine_qcl_and_fbsc(qcl_table: catalog.Table, fbsc_table: catalog.Table) -> pd.DataFrame:
    """Combine garden `faostat_qcl` and `faostat_fbsc` datasets.

    Parameters
    ----------
    qcl_table : catalog.Table
        Main table (in long format) of the `faostat_qcl` dataset.
    fbsc_table : catalog.Table
        Main table (in long format) of the `faostat_fbsc` dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data (as a dataframe, not a table).

    """
    columns = [
        "country",
        "year",
        "item_code",
        "element_code",
        "item",
        "element",
        "unit",
        "unit_short_name",
        "value",
        "population_with_data",
    ]
    qcl = pd.DataFrame(qcl_table).reset_index()[columns]
    # Select relevant element codes.
    qcl = qcl[qcl["element_code"].isin(ELEMENT_CODES_QCL)].reset_index(drop=True)
    qcl["value"] = qcl["value"].astype(float)
    qcl["element"] = [element for element in qcl["element"]]
    qcl["unit"] = [unit for unit in qcl["unit"]]
    qcl["item"] = [item for item in qcl["item"]]
    fbsc = pd.DataFrame(fbsc_table).reset_index()[columns]
    # Select relevant element codes.
    fbsc = fbsc[fbsc["element_code"].isin(ELEMENT_CODES_FBSC)].reset_index(drop=True)
    fbsc["value"] = fbsc["value"].astype(float)
    fbsc["element"] = [element for element in fbsc["element"]]
    fbsc["unit"] = [unit for unit in fbsc["unit"]]
    fbsc["item"] = [item for item in fbsc["item"]]

    rename_columns = {"item": "product"}
    combined = (
        dataframes.concatenate([qcl, fbsc], ignore_index=True).rename(columns=rename_columns).reset_index(drop=True)
    )

    # Sanity checks.
    assert len(combined) == (len(qcl) + len(fbsc)), "Unexpected number of rows after combining qcl and fbsc datasets."

    assert len(combined[combined["value"].isnull()]) == 0, "Unexpected nan values."

    n_items_per_item_code = combined.groupby("item_code")["product"].transform("nunique")
    assert combined[n_items_per_item_code > 1].empty, "There are item codes with multiple items."

    n_elements_per_element_code = combined.groupby("element_code")["element"].transform("nunique")
    assert combined[n_elements_per_element_code > 1].empty, "There are element codes with multiple elements."

    n_units_per_element_code = combined.groupby("element_code")["unit"].transform("nunique")
    assert combined[n_units_per_element_code > 1].empty, "There are element codes with multiple units."

    error = "There are unexpected duplicate rows. Rename items in custom_items.csv to avoid clashes."
    assert combined[combined.duplicated(subset=["product", "country", "year", "element", "unit"])].empty, error

    return cast(pd.DataFrame, combined)


def get_fao_population(combined: pd.DataFrame) -> pd.DataFrame:
    """Extract the FAO population data from data (in long format).

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of `faostat_qcl` and `faostat_fbsc` data (although this function could also be applied to just
        `faostat_fbsc` data, since `faostat_qcl` does not contain FAO population data).

    Returns
    -------
    fao_population : pd.DataFrame
        Population (by country and year) according to FAO, extracted from the `faostat_fbsc` dataset.

    """
    # Select the item and element that corresponds to population values.
    fao_population = combined[
        (combined["product"] == FAO_POPULATION_ITEM_NAME) & (combined["element"] == FAO_POPULATION_ELEMENT_NAME)
    ].reset_index(drop=True)

    # Check that population is given in "1000 persons" and convert to persons.
    error = "FAOSTAT population changed item, element, or unit."
    assert list(fao_population["unit"].unique()) == [FAO_POPULATION_UNIT], error
    fao_population["value"] *= 1000

    # Drop missing values and prepare output dataframe.
    fao_population = (
        fao_population[["country", "year", "value"]].dropna(how="any").rename(columns={"value": "fao_population"})
    )

    return fao_population


def process_combined_data(combined: pd.DataFrame) -> pd.DataFrame:
    """Process combined data (combination of `faostat_qcl` and `faostat_fbsc` data) to have the content and format
    required by the food explorer.

    Parameters
    ----------
    combined : pd.DataFrame
        Combination of `faostat_qcl` and `faostat_fbsc` data.

    Returns
    -------
    data_wide : pd.DataFrame
        Processed data (in wide format).

    """
    combined = combined.copy()

    # Get FAO population from data (it is given as another item).
    fao_population = get_fao_population(combined=combined)

    # List of all item codes to select.
    selected_item_codes = sorted(set(ITEM_CODES_FBSC).union(ITEM_CODES_QCL))

    # Check that all expected products are included in the data.
    missing_products = sorted(set(selected_item_codes) - set(set(combined["item_code"])))
    assert len(missing_products) == 0, f"{len(missing_products)} missing products for food explorer."

    # Select relevant products for the food explorer.
    combined = combined[combined["item_code"].isin(selected_item_codes)].reset_index(drop=True)

    # Join element and unit into one title column.
    combined["title"] = combined["element"] + " (" + combined["unit"] + ")"

    # This will create a table with just one column and country-year as index.
    index_columns = ["product", "country", "year"]
    data_wide = combined.pivot(index=index_columns, columns=["title"], values="value").reset_index()

    # Add column for FAO population.
    data_wide = pd.merge(data_wide, fao_population, on=["country", "year"], how="left")

    # Add column for OWID population.
    data_wide = geo.add_population_to_dataframe(df=data_wide, warn_on_missing_countries=False)

    # Fill gaps in OWID population with FAO population (for "* (FAO)" countries, i.e. countries that were not
    # harmonized and for which there is no OWID population).
    # Then drop "fao_population", since it is no longer needed.
    data_wide["population"] = data_wide["population"].fillna(data_wide["fao_population"])
    data_wide = data_wide.drop(columns="fao_population")

    assert len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0, "Unexpected columns with only nan values."

    # Set a reasonable index.
    data_wide = data_wide.set_index(index_columns, verify_integrity=True)

    return data_wide


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Fetch the dataset short name from dest_dir.
    dataset_short_name = Path(dest_dir).name

    # Define path to current step file.
    current_step_file = (CURRENT_DIR / dataset_short_name).with_suffix(".py")

    # Get paths and naming conventions for current data step.
    paths = PathFinder(current_step_file.as_posix())

    # Load latest qcl and fbsc datasets from garden.
    qcl_dataset: catalog.Dataset = paths.load_dependency(f"{NAMESPACE}_qcl")
    fbsc_dataset: catalog.Dataset = paths.load_dependency(f"{NAMESPACE}_fbsc")

    # Get main long tables from qcl and fbsc datasets.
    qcl_table = qcl_dataset[f"{NAMESPACE}_qcl"]
    fbsc_table = fbsc_dataset[f"{NAMESPACE}_fbsc"]

    #
    # Process data.
    #
    # Combine qcl and fbsc data.
    data = combine_qcl_and_fbsc(qcl_table=qcl_table, fbsc_table=fbsc_table)

    # Prepare data in the format required by the food explorer.
    data = process_combined_data(combined=data)

    # Create table of products.
    table = catalog.Table(data, short_name=dataset_short_name)

    #
    # Save outputs.
    #
    # Initialise new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[table], default_metadata=fbsc_dataset.metadata)

    # Update dataset metadata and combine sources from qcl and fbsc datasets.
    ds_garden.metadata.title = DATASET_TITLE
    ds_garden.metadata.description = DATASET_DESCRIPTION
    ds_garden.metadata.sources = fbsc_dataset.metadata.sources + qcl_dataset.metadata.sources

    # Create new dataset in garden.
    ds_garden.save()
