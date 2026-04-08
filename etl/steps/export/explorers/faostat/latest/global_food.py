"""Step that pushes the global-food explorer (tsv content) to DB, to create the global-food (indicator-based) explorer.."""

from copy import deepcopy
from typing import Any, Callable, Dict, Optional

from owid.catalog.utils import underscore
from structlog import get_logger

from etl.collection import combine_config_dimensions, expand_config
from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Items from faostat_qcl to include.
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
    "00000771",  # From faostat_qcl - 'Flax, raw or retted' (previously 'Flax fibre').
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
    # "00000667",  # From faostat_qcl - 'Tea leaves' (previously 'Tea').
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

# Items from faostat_fbsc to include.
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

# Elements from faostat_qcl to include.
ELEMENT_CODES_QCL = [
    "005510",  # Production (tonnes).
    "5510pe",  # Production per capita (kilograms per capita).
    "005312",  # Area harvested (hectares).
    "5312pe",  # Area harvested per capita (square meters per capita).
    "005320",  # Producing or slaughtered animals (animals).
    "5320pc",  # Producing or slaughtered animals per capita (animals per capita).
    "005321",  # Producing or slaughtered animals (animals).
    "5321pc",  # Producing or slaughtered animals per capita (animals per capita).
    "005413",  # Eggs per bird (eggs per bird).
    "005412",  # Yield (tonnes per hectare).
    "005417",  # Yield (kilograms per animal).
    "005424",  # Yield (kilograms per animal).
    "005513",  # Eggs produced (eggs).
    "005313",  # Laying (animals).
    "005318",  # Milk animals (animals).
]

# Elements from faostat_fbsc to include.
ELEMENT_CODES_FBSC = [
    "0645pc",  # Food available for consumption (kilograms per year per capita)
    "0645pe",  # Food available for consumption (grams per day per capita)
    "0664pc",  # Food available for consumption (kilocalories per day per capita)
    "0674pc",  # Food available for consumption (grams of protein per day per capita)
    "0684pc",  # Food available for consumption (grams of fat per day per capita)
    "005142",  # Food (tonnes)
    "5142pe",  # Food (kilograms per capita)
    "005301",  # Domestic supply (tonnes)
    "5301pe",  # Domestic supply (kilograms per capita)
    "005521",  # Feed (tonnes)
    "5521pe",  # Feed (kilograms per capita)
    "005611",  # Imports (tonnes)
    "5611pe",  # Imports (kilograms per capita)
    "005911",  # Exports (tonnes)
    "5911pe",  # Exports (kilograms per capita)
    "005123",  # Waste in supply chain (tonnes)
    "5123pe",  # Waste in supply chain (tonnes_per_capita)
    "005154",  # Other uses (tonnes)
    "5154pe",  # Other uses (kilograms per capita)
    # Element 'Production' (in tonnes, originally given in 1000 tonnes) is taken from qcl.
    # Although fbsc has items for this element that are not in qcl, they overlap in a number of items with slightly
    # different values. To avoid this issue, we ignore the element from fbsc and use only the one in qcl.
    # '005511',
    # The following element code is for population.
    # "000511",  # Population
    # Indicators initially given by FAOSTAT as per capita, and converted to total by OWID:
    # Food supply quantity (kg/capita/yr).
    # "000645",
    # Food supply (kcal/capita/day).
    # "000664",
    # Protein supply quantity (g/capita/day)
    # "000674",
    # Fat supply quantity (g/capita/day).
    # "000684",
    # Other elements not used.
    # "005072",  # Stock variation (tonnes)
    # "005131",  # Processing (tonnes)
    # "005170",  # Residuals (tonnes)
    # "005171",  # Tourist consumption (tonnes)
    # "005527",  # Seed (tonnes)
]


def prepare_table_with_dimensions(tb, item_codes, element_codes):
    columns_to_drop = []
    UNITS_IN_RADIO_BUTTONS = [
        "kilograms per year",
        "grams per day",
        "kilocalories per day",
        "grams of protein per day",
        "grams of fat per day",
    ]
    UNITS_NOT_IN_RADIO_BUTTONS = [
        "hectares",
        "tonnes",
        "kilograms",
        "square meters",
        "tonnes per hectare",
        "animals",
        "kilograms per animal",
        "eggs per bird",
        "eggs",
    ]
    for column in tb.drop(columns=["country", "year"]).columns:
        item, item_code, element, element_code, unit = sum(
            [[j.strip() for j in i.split("|")] for i in tb[column].metadata.title.split("||")], []
        )
        if (item_code in item_codes) and (element_code in element_codes):
            unit = unit.replace(" per capita", "")
            if unit in UNITS_IN_RADIO_BUTTONS:
                pass
            elif unit in UNITS_NOT_IN_RADIO_BUTTONS:
                # We remove the unit in these cases, so they don't appear in the dropdown.
                unit = ""
            else:
                raise ValueError(f"Unexpected unit {unit}")
            tb[column].metadata.dimensions = {
                "item": underscore(item),
                "metric": underscore(element),
                "unit": underscore(unit, validate=False),
                "per_capita": True if ("pc" in element_code) or ("pe" in element_code) else False,
            }
            tb[column].metadata.original_short_name = "value"
        else:
            columns_to_drop.append(column)

    # Drop unnecessary columns.
    tb = tb.drop(columns=columns_to_drop)

    return tb


def _humanize_dimension_names(dimension, transformation, replacements):
    for field, value in dimension.items():
        if field == "name":
            if value in replacements:
                dimension["name"] = replacements[value]
            else:
                dimension["name"] = transformation(value)
        if field == "choices":
            for choice in value:
                _humanize_dimension_names(choice, transformation=transformation, replacements=replacements)


def default_slug_name_transformation(slug):
    """Default transformation of machine-readable slugs, e.g. "area_harvested", into human-readable dimension names, e.g. "Area harvested"."""
    return slug.replace("__", ", ").replace("_", " ").capitalize()


def humanize_dimension_names_in_config(
    config: Dict[str, Any],
    transformation: Optional[Callable[[str], str]] = None,
    replacements: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Transform all machine-readable slugs, e.g. "area_harvested" into human-readable names, e.g. "Area harvested"."""
    if transformation is None:
        transformation = default_slug_name_transformation

    if replacements is None:
        replacements = dict()

    config_new = deepcopy(config)
    for dimension in config_new["dimensions"]:
        _humanize_dimension_names(dimension, transformation=transformation, replacements=replacements)

    return config_new


def append_to_config(config, tb):
    table_config = expand_config(
        tb=tb,
        dimensions=["item", "metric", "unit", "per_capita"],
        common_view_config={"tab": "map"},
    )
    config = deepcopy(config)

    # Append dimensions to config.
    if "dimensions" not in config:
        config["dimensions"] = table_config["dimensions"]
    else:
        config["dimensions"] = combine_config_dimensions(config["dimensions"], table_config["dimensions"])

    # Append views to existing config.
    if "views" not in config:
        config["views"] = table_config["views"]
    else:
        config["views"] += table_config["views"]

    return config


def set_default_view(config, default_view):
    _default_view_set = False
    for view in config["views"]:
        if view["dimensions"] == default_view:
            # NOTE: A copy seems to be necessary, otherwise all common configs will be modified to have a default view.
            view["config"] = deepcopy(view.get("config", {}))
            view["config"]["defaultView"] = True
            _default_view_set = True
            break
    if not _default_view_set:
        log.warning("Default view not found.")

    return config


def run():
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl_flat", load_data=False)

    # Load FAOSTAT FBSC dataset (combination of FBS and FBSH), and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc_flat", load_data=False)

    # Load grapher config from YAML.
    config = paths.load_collection_config()

    #
    # Process data.
    #
    # Some columns have very sparse data (in some cases, just nans and zero except a few values close to zero, possibly spurious).
    # These are uninformative, and show a red map, so I'll remove them here.
    tb_fbsc = tb_fbsc.drop(
        columns=[
            "apples__00002617__other_uses__005154__tonnes",
            "apples__00002617__other_uses__5154pe__kilograms_per_capita",
            "sesame_oil__00002579__food_available_for_consumption__0674pc__grams_of_protein_per_day_per_capita",
        ],
    )

    # Prepare tables with dimensions.
    tb_qcl = prepare_table_with_dimensions(tb=tb_qcl, item_codes=ITEM_CODES_QCL, element_codes=ELEMENT_CODES_QCL)
    tb_fbsc = prepare_table_with_dimensions(tb=tb_fbsc, item_codes=ITEM_CODES_FBSC, element_codes=ELEMENT_CODES_FBSC)

    # Append dimensions and views from QCL and FBSC to config.
    config = append_to_config(config=config, tb=tb_qcl)
    config = append_to_config(config=config, tb=tb_fbsc)

    # Improve names of certain dimensions and choices.
    config = humanize_dimension_names_in_config(
        config=config,
        replacements={
            "item": "Food",
            "maize": "Maize (corn)",
            "maize_oil": "Maize (corn) oil",
            "area_harvested": "Land use",
            "feed": "Allocated to animal feed",
            "food": "Allocated to human food",
            "other_uses": "Allocated to other uses",
            "eggs_from_other_birds__excl__hens": "Eggs from other birds",
            "eggs": "Eggs, total",
            "animal_fats": "Fat, total",
            "offal": "Offal, total",
            "meat__ass": "Meat, donkey",
            "total": "All food",
            "chillies_and_peppers": "Chilies and peppers",
            "fat__sheep": "Fat, sheep",
            "fat__buffaloes": "Fat, buffalo",
            "fat__pigs": "Fat, pig",
            "fat__camels": "Fat, camel",
            "fat__goats": "Fat, goat",
            "fat__cattle": "Fat, cattle",
            "laying": "Laying birds",
        },
    )

    # Set defalt view.
    config = set_default_view(
        config=config,
        default_view={
            "item": "maize",
            "metric": "production",
            "unit": "",
            "per_capita": "False",
        },
    )

    # Sort food and metric elements in the dropdown alphabetically.
    for dropdown_i in [0, 1]:
        # NOTE: Unfortunately, this is only achievable for the first dropdown. The order of any subsequent dropdowns is determined by the first time that the choices appear. So, currently, the first item in the first dropdown, "All food" starts with metric "Food available for consumption", which means that this will always be the first element in the "Metric" dropdown.
        config["dimensions"][dropdown_i]["choices"] = sorted(
            config["dimensions"][dropdown_i]["choices"], key=lambda x: x["name"]
        )

    # Make per capita a checkbox and unit a radio button.
    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}
        if dimension["slug"] == "unit":
            dimension["presentation"] = {"type": "radio"}

    #
    # Save outputs.
    #
    # Initialize a new explorer.
    c = paths.create_collection(
        config=config,
        short_name="global-food",
        explorer=True,
    )

    # Save explorer.
    c.save(tolerate_extra_indicators=True)
