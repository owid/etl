"""Step that pushes the global-food explorer (tsv content) to DB, to create the global-food (indicator-based) explorer."""

from structlog import get_logger

from etl.collections.explorer import expand_config
from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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

# List of element codes to consider from faostat_qcl.
ELEMENT_CODES_QCL = [
    "005312",
    "005313",
    # "005314",
    "005318",
    "005320",
    "005321",
    # "005410",
    "005413",
    "005417",
    "005412",
    # "005420",
    # "005422",
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


def run():
    #
    # Load inputs.
    #
    # Load FAOSTAT QCL dataset, and read its main table.
    ds_qcl = paths.load_dataset("faostat_qcl")
    tb_qcl = ds_qcl.read("faostat_qcl_flat")

    # Load FAOSTAT FBSC dataset (combination of FBS and FBSH), and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc_flat")

    # Load grapher config from YAML.
    config = paths.load_explorer_config()

    #
    # Process data.
    #
    columns_to_drop = []
    for column in tb_fbsc.drop(columns=["country", "year"]).columns:
        item, item_code, element, element_code, unit = sum(
            [[j.strip() for j in i.split("|")] for i in tb_fbsc[column].metadata.title.split("||")], []
        )
        unit = unit.replace(" per capita", "")
        if (item_code in ITEM_CODES_FBSC) and (element_code in ELEMENT_CODES_FBSC):
            tb_fbsc[column].metadata.dimensions = {
                "food": item,
                "metric": element,
                "unit": unit,
                "per_capita": True if "pc" in element_code else False,
            }
            tb_fbsc[column].metadata.original_short_name = column
        else:
            columns_to_drop.append(column)
    tb_fbsc = tb_fbsc.drop(columns=columns_to_drop)

    columns_to_drop = []
    for column in tb_qcl.drop(columns=["country", "year"]).columns:
        item, item_code, element, element_code, unit = sum(
            [[j.strip() for j in i.split("|")] for i in tb_qcl[column].metadata.title.split("||")], []
        )
        unit = unit.replace(" per capita", "")
        if (item_code in ITEM_CODES_QCL) and (element_code in ELEMENT_CODES_QCL):
            tb_qcl[column].metadata.dimensions = {
                "food": item,
                "metric": element,
                "unit": unit,
                "per_capita": True if "pc" in element_code else False,
            }
            tb_qcl[column].metadata.original_short_name = column
        else:
            columns_to_drop.append(column)
    tb_qcl = tb_qcl.drop(columns=columns_to_drop)

    # Expand configuration to get all dimensions and views from tables.
    config_new = expand_config(
        [tb_qcl, tb_fbsc],
        default_view={
            "food": "Apples",
            "metric": "Area harvested",
            "unit": "hectares",
            "per_capita": "False",
        },
    )
    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]

    # Make per capita a checkbox and unit a radio button.
    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["name"] = "per capita"
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}
        if dimension["slug"] == "unit":
            dimension["presentation"] = {"type": "radio"}

    #
    # Save outputs.
    #
    # Initialize a new explorer.
    ds_explorer = paths.create_explorer(config=config, explorer_name="global-food")

    # Save explorer.
    ds_explorer.save(tolerate_extra_indicators=True)
