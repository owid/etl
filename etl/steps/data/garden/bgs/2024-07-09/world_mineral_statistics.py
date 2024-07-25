"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert carats to metric tonnes.
CARATS_TO_TONNES = 2e-7
# Convert million cubic meters of helium to metric tonnes.
MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES = 178.5
# Convert million cubic meters of natural gas to metric tonnes.
# TODO: Figure out if this is a good idea, otherwise keep in million cubic meters.
MILLION_CUBIC_METERS_OF_NATURAL_GAS_TO_TONNES = 800

# Harmonize commodity-subcommodity names.
# NOTE: This list should contain all commodity-subcommodity pairs expected in the data.
# Set to None any commodity-subcommodity that should not be included.
COMMODITY_MAPPING = {
    ("Aggregates, primary", "Crushed rock"): ("Aggregates, primary", "Crushed rock"),
    ("Aggregates, primary", "Sand and gravel"): ("Aggregates, primary", "Sand and gravel"),
    ("Aggregates, primary", "Total"): ("Aggregates, primary", "Total"),
    ("Alumina", "Total"): ("Alumina", "Total"),
    ("Aluminium, primary", "Total"): ("Aluminium, primary", "Total"),
    ("Antimony", "Crude"): ("Antimony", "Crude"),
    ("Antimony", "Crude & regulus"): ("Antimony", "Crude & regulus"),
    ("Antimony", "Liquated"): ("Antimony", "Liquated"),
    ("Antimony", "Metal"): ("Antimony", "Metal"),
    ("Antimony", "Ores & concentrates"): ("Antimony", "Ores & concentrates"),
    ("Antimony", "Oxide"): ("Antimony", "Oxide"),
    ("Antimony", "Refined & regulus"): ("Antimony", "Refined & regulus"),
    ("Antimony", "Regulus"): ("Antimony", "Regulus"),
    ("Antimony", "Sulfide"): ("Antimony", "Sulfide"),
    ("Antimony, mine", "Total"): ("Antimony", "Mine production"),
    ("Arsenic", "Metallic arsenic"): ("Arsenic", "Metallic arsenic"),
    ("Arsenic", "Total"): ("Arsenic", "Total"),
    ("Arsenic", "White arsenic"): ("Arsenic", "White arsenic"),
    ("Arsenic, white", "Total"): ("Arsenic, white", "Total"),
    ("Asbestos", "Amosite"): ("Asbestos", "Amosite"),
    ("Asbestos", "Amphibole"): ("Asbestos", "Amphibole"),
    ("Asbestos", "Anthophyllite"): ("Asbestos", "Anthophyllite"),
    ("Asbestos", "Chrysotile"): ("Asbestos", "Chrysotile"),
    ("Asbestos", "Crocidolite"): ("Asbestos", "Crocidolite"),
    ("Asbestos", "Total"): ("Asbestos", "Total"),
    ("Asbestos, unmanufactured", "Amosite"): ("Asbestos, unmanufactured", "Amosite"),
    ("Asbestos, unmanufactured", "Chrysotile"): ("Asbestos, unmanufactured", "Chrysotile"),
    ("Asbestos, unmanufactured", "Crocidolite"): ("Asbestos, unmanufactured", "Crocidolite"),
    ("Asbestos, unmanufactured", "Other manufactured"): ("Asbestos, unmanufactured", "Other manufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured"): ("Asbestos, unmanufactured", "Unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured, crude"): ("Asbestos, unmanufactured", "Unmanufactured, crude"),
    ("Asbestos, unmanufactured", "Unmanufactured, fibre"): ("Asbestos, unmanufactured", "Unmanufactured, fibre"),
    ("Asbestos, unmanufactured", "Unmanufactured, shorts"): ("Asbestos, unmanufactured", "Unmanufactured, shorts"),
    ("Asbestos, unmanufactured", "Unmanufactured, waste"): ("Asbestos, unmanufactured", "Unmanufactured, waste"),
    ("Asbestos, unmanufactured", "Waste"): ("Asbestos, unmanufactured", "Waste"),
    ("Barytes", "Barium minerals"): ("Barytes", "Barium minerals"),
    ("Barytes", "Barytes"): ("Barytes", "Barytes"),
    ("Barytes", "Total"): ("Barytes", "Total"),
    ("Barytes", "Witherite"): ("Barytes", "Witherite"),
    ("Bauxite", "Total"): ("Bauxite", "Total"),
    ("Bauxite, alumina and aluminium", "Alumina"): ("Bauxite, alumina and aluminium", "Alumina"),
    ("Bauxite, alumina and aluminium", "Alumina hydrate"): ("Bauxite, alumina and aluminium", "Alumina hydrate"),
    ("Bauxite, alumina and aluminium", "Bauxite"): ("Bauxite, alumina and aluminium", "Bauxite"),
    ("Bauxite, alumina and aluminium", "Bauxite, calcined"): ("Bauxite, alumina and aluminium", "Bauxite, calcined"),
    ("Bauxite, alumina and aluminium", "Bauxite, crude dried"): (
        "Bauxite, alumina and aluminium",
        "Bauxite, crude dried",
    ),
    ("Bauxite, alumina and aluminium", "Bauxite, dried"): ("Bauxite, alumina and aluminium", "Bauxite, dried"),
    ("Bauxite, alumina and aluminium", "Bauxite, uncalcined"): (
        "Bauxite, alumina and aluminium",
        "Bauxite, uncalcined",
    ),
    ("Bauxite, alumina and aluminium", "Scrap"): ("Bauxite, alumina and aluminium", "Scrap"),
    ("Bauxite, alumina and aluminium", "Unwrought"): ("Bauxite, alumina and aluminium", "Unwrought"),
    ("Bauxite, alumina and aluminium", "Unwrought & scrap"): ("Bauxite, alumina and aluminium", "Unwrought & scrap"),
    ("Bauxite, alumina and aluminium", "Unwrought alloys"): ("Bauxite, alumina and aluminium", "Unwrought alloys"),
    ("Bentonite and fuller's earth", "Attapulgite"): ("Bentonite and fuller's earth", "Attapulgite"),
    ("Bentonite and fuller's earth", "Bentonite"): ("Bentonite and fuller's earth", "Bentonite"),
    ("Bentonite and fuller's earth", "Fuller's earth"): ("Bentonite and fuller's earth", "Fuller's earth"),
    ("Bentonite and fuller's earth", "Sepiolite"): ("Bentonite and fuller's earth", "Sepiolite"),
    ("Bentonite and fuller's earth", "Total"): ("Bentonite and fuller's earth", "Total"),
    ("Beryl", "Total"): ("Beryl", "Total"),
    ("Bismuth", "Compounds"): ("Bismuth", "Compounds"),
    ("Bismuth", "Metal"): ("Bismuth", "Metal"),
    ("Bismuth", "Ores & concentrates"): ("Bismuth", "Ores & concentrates"),
    ("Bismuth, mine", "Total"): ("Bismuth, mine", "Total"),
    ("Borates", "Total"): ("Borates", "Total"),
    ("Bromine", "Compounds"): ("Bromine", "Compounds"),
    ("Bromine", "Total"): ("Bromine", "Total"),
    ("Cadmium", "Metal"): ("Cadmium", "Metal"),
    ("Cadmium", "Other"): ("Cadmium", "Other"),
    ("Cadmium", "Oxide"): ("Cadmium", "Oxide"),
    ("Cadmium", "Sulfide"): ("Cadmium", "Sulfide"),
    ("Cadmium", "Total"): ("Cadmium", "Total"),
    ("Cement", "Cement clinkers"): ("Cement", "Cement clinkers"),
    ("Cement", "Other cement"): ("Cement", "Other cement"),
    ("Cement", "Portland cement"): ("Cement", "Portland cement"),
    ("Cement  clinker", "Cement, clinker"): ("Cement  clinker", "Cement, clinker"),
    ("Cement, finished", "Cement, finished"): ("Cement, finished", "Cement, finished"),
    ("Chromium", "Metal"): ("Chromium", "Metal"),
    ("Chromium", "Ores & concentrates"): ("Chromium", "Ores & concentrates"),
    ("Chromium ores and concentrates", "Total"): ("Chromium ores and concentrates", "Total"),
    ("Coal", "Anthracite"): ("Coal", "Anthracite"),
    ("Coal", "Anthracite & Bituminous"): ("Coal", "Anthracite & Bituminous"),
    ("Coal", "Anthracite & semi-bituminous"): ("Coal", "Anthracite & semi-bituminous"),
    ("Coal", "Anthracite (mined)"): ("Coal", "Anthracite (mined)"),
    ("Coal", "Anthracite (opencast)"): ("Coal", "Anthracite (opencast)"),
    ("Coal", "Bituminous"): ("Coal", "Bituminous"),
    ("Coal", "Bituminous & lignite"): ("Coal", "Bituminous & lignite"),
    ("Coal", "Bituminous (mined)"): ("Coal", "Bituminous (mined)"),
    ("Coal", "Bituminous (opencast)"): ("Coal", "Bituminous (opencast)"),
    ("Coal", "Briquettes"): ("Coal", "Briquettes"),
    ("Coal", "Brown coal"): ("Coal", "Brown coal"),
    ("Coal", "Brown coal & lignite"): ("Coal", "Brown coal & lignite"),
    ("Coal", "Coal"): ("Coal", "Coal"),
    ("Coal", "Coking coal"): ("Coal", "Coking coal"),
    ("Coal", "Hard coal"): ("Coal", "Hard coal"),
    ("Coal", "Lignite"): ("Coal", "Lignite"),
    ("Coal", "Other bituminous coal"): ("Coal", "Other bituminous coal"),
    ("Coal", "Other coal"): ("Coal", "Other coal"),
    ("Coal", "Sub-bituminous"): ("Coal", "Sub-bituminous"),
    ("Coal", "Total"): ("Coal", "Total"),
    ("Cobalt", "Metal & refined"): ("Cobalt", "Metal & refined"),
    ("Cobalt", "Ore"): ("Cobalt", "Ore"),
    ("Cobalt", "Oxide, sinter & sulfide"): ("Cobalt", "Oxide, sinter & sulfide"),
    ("Cobalt", "Oxides"): ("Cobalt", "Oxides"),
    ("Cobalt", "Salts"): ("Cobalt", "Salts"),
    ("Cobalt", "Scrap"): ("Cobalt", "Scrap"),
    ("Cobalt", "Total"): ("Cobalt", "Total"),
    ("Cobalt", "Unwrought"): ("Cobalt", "Unwrought"),
    ("Cobalt, mine", "Total"): ("Cobalt, mine", "Total"),
    ("Cobalt, refined", "Total"): ("Cobalt, refined", "Total"),
    ("Copper", "Ash and residues"): ("Copper", "Ash and residues"),
    ("Copper", "Burnt cupreous pyrites"): ("Copper", "Burnt cupreous pyrites"),
    ("Copper", "Cement copper"): ("Copper", "Cement copper"),
    ("Copper", "Matte"): ("Copper", "Matte"),
    ("Copper", "Matte & Scrap"): ("Copper", "Matte & Scrap"),
    ("Copper", "Matte & cement"): ("Copper", "Matte & cement"),
    ("Copper", "Ore & matte"): ("Copper", "Ore & matte"),
    ("Copper", "Ores & concentrates"): ("Copper", "Ores & concentrates"),
    ("Copper", "Ores, concentrates & matte"): ("Copper", "Ores, concentrates & matte"),
    ("Copper", "Scrap"): ("Copper", "Scrap"),
    ("Copper", "Sludge, slimes & residues"): ("Copper", "Sludge, slimes & residues"),
    ("Copper", "Total"): ("Copper", "Total"),
    ("Copper", "Unwrought"): ("Copper", "Unwrought"),
    ("Copper", "Unwrought & Scrap"): ("Copper", "Unwrought & Scrap"),
    ("Copper", "Unwrought alloys"): ("Copper", "Unwrought alloys"),
    ("Copper", "Unwrought, matte, cement & refined"): ("Copper", "Unwrought, matte, cement & refined"),
    ("Copper", "Unwrought, refined"): ("Copper", "Unwrought, refined"),
    ("Copper", "Unwrought, unrefined"): ("Copper", "Unwrought, unrefined"),
    ("Copper, mine", "Total"): ("Copper, mine", "Total"),
    ("Copper, refined", "Total"): ("Copper, refined", "Total"),
    ("Copper, smelter", "Total"): ("Copper, smelter", "Total"),
    ("Diamond", "Cut"): ("Diamond", "Cut"),
    ("Diamond", "Dust"): ("Diamond", "Dust"),
    ("Diamond", "Gem"): ("Diamond", "Gem"),
    ("Diamond", "Gem, cut"): ("Diamond", "Gem, cut"),
    ("Diamond", "Gem, rough"): ("Diamond", "Gem, rough"),
    ("Diamond", "Industrial"): ("Diamond", "Industrial"),
    ("Diamond", "Other"): ("Diamond", "Other"),
    ("Diamond", "Rough"): ("Diamond", "Rough"),
    ("Diamond", "Rough & Cut"): ("Diamond", "Rough & Cut"),
    ("Diamond", "Total"): ("Diamond", "Total"),
    ("Diamond", "Unsorted"): ("Diamond", "Unsorted"),
    ("Diatomite", "Activated diatomite"): ("Diatomite", "Activated diatomite"),
    ("Diatomite", "Moler"): ("Diatomite", "Moler"),
    ("Diatomite", "Moler bricks"): ("Diatomite", "Moler bricks"),
    ("Diatomite", "Total"): ("Diatomite", "Total"),
    ("Feldspar", "Total"): ("Feldspar", "Total"),
    ("Ferro-alloys", "Calcium silicide"): ("Ferro-alloys", "Calcium silicide"),
    ("Ferro-alloys", "Fe-Ti, Fe-W, Fe-Mo, Fe-V"): ("Ferro-alloys", "Fe-Ti, Fe-W, Fe-Mo, Fe-V"),
    ("Ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"): ("Ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"),
    ("Ferro-alloys", "Ferro-Alloys (Ferro-silicon & silicon metal)"): (
        "Ferro-alloys",
        "Ferro-Alloys (Ferro-silicon & silicon metal)",
    ),
    ("Ferro-alloys", "Ferro-Si-manganese & silico-speigeleisen"): (
        "Ferro-alloys",
        "Ferro-Si-manganese & silico-speigeleisen",
    ),
    ("Ferro-alloys", "Ferro-alloys"): ("Ferro-alloys", "Ferro-alloys"),
    ("Ferro-alloys", "Ferro-aluminium"): ("Ferro-alloys", "Ferro-aluminium"),
    ("Ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): (
        "Ferro-alloys",
        "Ferro-aluminium & ferro-silico-aluminium",
    ),
    ("Ferro-alloys", "Ferro-calcium-silicon"): ("Ferro-alloys", "Ferro-calcium-silicon"),
    ("Ferro-alloys", "Ferro-chrome"): ("Ferro-alloys", "Ferro-chrome"),
    ("Ferro-alloys", "Ferro-chrome & ferro-silico-chrome"): ("Ferro-alloys", "Ferro-chrome & ferro-silico-chrome"),
    ("Ferro-alloys", "Ferro-manganese"): ("Ferro-alloys", "Ferro-manganese"),
    ("Ferro-alloys", "Ferro-manganese & ferro-silico-manganese"): (
        "Ferro-alloys",
        "Ferro-manganese & ferro-silico-manganese",
    ),
    ("Ferro-alloys", "Ferro-manganese & spiegeleisen"): ("Ferro-alloys", "Ferro-manganese & spiegeleisen"),
    ("Ferro-alloys", "Ferro-molybdenum"): ("Ferro-alloys", "Ferro-molybdenum"),
    ("Ferro-alloys", "Ferro-nickel"): ("Ferro-alloys", "Ferro-nickel"),
    ("Ferro-alloys", "Ferro-niobium"): ("Ferro-alloys", "Ferro-niobium"),
    ("Ferro-alloys", "Ferro-phosphorus"): ("Ferro-alloys", "Ferro-phosphorus"),
    ("Ferro-alloys", "Ferro-rare earth"): ("Ferro-alloys", "Ferro-rare earth"),
    ("Ferro-alloys", "Ferro-silico-calcium-aluminium"): ("Ferro-alloys", "Ferro-silico-calcium-aluminium"),
    ("Ferro-alloys", "Ferro-silico-chrome"): ("Ferro-alloys", "Ferro-silico-chrome"),
    ("Ferro-alloys", "Ferro-silico-magnesium"): ("Ferro-alloys", "Ferro-silico-magnesium"),
    ("Ferro-alloys", "Ferro-silico-manganese"): ("Ferro-alloys", "Ferro-silico-manganese"),
    ("Ferro-alloys", "Ferro-silicon"): ("Ferro-alloys", "Ferro-silicon"),
    ("Ferro-alloys", "Ferro-titanium"): ("Ferro-alloys", "Ferro-titanium"),
    ("Ferro-alloys", "Ferro-titanium & ferro-silico-titanium"): (
        "Ferro-alloys",
        "Ferro-titanium & ferro-silico-titanium",
    ),
    ("Ferro-alloys", "Ferro-tungsten"): ("Ferro-alloys", "Ferro-tungsten"),
    ("Ferro-alloys", "Ferro-vanadium"): ("Ferro-alloys", "Ferro-vanadium"),
    ("Ferro-alloys", "Other ferro-alloys"): ("Ferro-alloys", "Other ferro-alloys"),
    ("Ferro-alloys", "Pig iron"): ("Ferro-alloys", "Pig iron"),
    ("Ferro-alloys", "Silicon metal"): ("Ferro-alloys", "Silicon metal"),
    ("Ferro-alloys", "Silicon pig iron"): ("Ferro-alloys", "Silicon pig iron"),
    ("Ferro-alloys", "Spiegeleisen"): ("Ferro-alloys", "Spiegeleisen"),
    ("Fluorspar", "Total"): ("Fluorspar", "Total"),
    ("Gallium, primary", "Total"): ("Gallium, primary", "Total"),
    ("Gemstones", "Total"): ("Gemstones", "Total"),
    ("Germanium metal", "Total"): ("Germanium metal", "Total"),
    ("Gold", "Metal"): ("Gold", "Metal"),
    ("Gold", "Metal, other"): ("Gold", "Metal, other"),
    ("Gold", "Metal, refined"): ("Gold", "Metal, refined"),
    ("Gold", "Metal, unrefined"): ("Gold", "Metal, unrefined"),
    ("Gold", "Ores & concentrates"): ("Gold", "Ores & concentrates"),
    ("Gold", "Ores, concentrates & unrefined metal"): ("Gold", "Ores, concentrates & unrefined metal"),
    ("Gold", "Waste & scrap"): ("Gold", "Waste & scrap"),
    ("Gold, mine", "Total"): ("Gold, mine", "Total"),
    ("Graphite", "Total"): ("Graphite", "Total"),
    ("Gypsum and plaster", "Anhydrite"): ("Gypsum and plaster", "Anhydrite"),
    ("Gypsum and plaster", "Calcined"): ("Gypsum and plaster", "Calcined"),
    ("Gypsum and plaster", "Crede & ground"): ("Gypsum and plaster", "Crede & ground"),
    ("Gypsum and plaster", "Crude"): ("Gypsum and plaster", "Crude"),
    ("Gypsum and plaster", "Crude & calcined"): ("Gypsum and plaster", "Crude & calcined"),
    ("Gypsum and plaster", "Ground & calcined"): ("Gypsum and plaster", "Ground & calcined"),
    ("Gypsum and plaster", "Total"): ("Gypsum and plaster", "Total"),
    ("Helium", "Helium"): ("Helium", "Helium"),
    ("Indium, refinery", "Total"): ("Indium, refinery", "Total"),
    ("Iodine", "Total"): ("Iodine", "Total"),
    ("Iron ore", "Burnt pyrites"): ("Iron ore", "Burnt pyrites"),
    ("Iron ore", "Total"): ("Iron ore", "Total"),
    ("Iron, pig", "Total"): ("Iron, pig", "Total"),
    ("Iron, steel and ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"): (
        "Iron, steel and ferro-alloys",
        "Fe-silico-spiegeleisen & Si-Mn",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-Si-manganese & silico-speigeleisen"): (
        "Iron, steel and ferro-alloys",
        "Ferro-Si-manganese & silico-speigeleisen",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-alloys"): ("Iron, steel and ferro-alloys", "Ferro-alloys"),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium"): ("Iron, steel and ferro-alloys", "Ferro-aluminium"),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): (
        "Iron, steel and ferro-alloys",
        "Ferro-aluminium & ferro-silico-aluminium",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium, Fe-Si-Al & Fe-Si-Mn-Al"): (
        "Iron, steel and ferro-alloys",
        "Ferro-aluminium, Fe-Si-Al & Fe-Si-Mn-Al",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-calcium-silicon"): (
        "Iron, steel and ferro-alloys",
        "Ferro-calcium-silicon",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-chrome"): ("Iron, steel and ferro-alloys", "Ferro-chrome"),
    ("Iron, steel and ferro-alloys", "Ferro-chrome & ferro-silico-chrome"): (
        "Iron, steel and ferro-alloys",
        "Ferro-chrome & ferro-silico-chrome",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-manganese"): ("Iron, steel and ferro-alloys", "Ferro-manganese"),
    ("Iron, steel and ferro-alloys", "Ferro-manganese & Fe-Si-Mn"): (
        "Iron, steel and ferro-alloys",
        "Ferro-manganese & Fe-Si-Mn",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-manganese & spiegeleisen"): (
        "Iron, steel and ferro-alloys",
        "Ferro-manganese & spiegeleisen",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-molybdenum"): ("Iron, steel and ferro-alloys", "Ferro-molybdenum"),
    ("Iron, steel and ferro-alloys", "Ferro-nickel"): ("Iron, steel and ferro-alloys", "Ferro-nickel"),
    ("Iron, steel and ferro-alloys", "Ferro-niobium"): ("Iron, steel and ferro-alloys", "Ferro-niobium"),
    ("Iron, steel and ferro-alloys", "Ferro-niobium & ferro-niobium-tantalum"): (
        "Iron, steel and ferro-alloys",
        "Ferro-niobium & ferro-niobium-tantalum",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-phosphorus"): ("Iron, steel and ferro-alloys", "Ferro-phosphorus"),
    ("Iron, steel and ferro-alloys", "Ferro-silico-aluminium, Fe-Si-Mn-Al, Fe-Si-Al-Ca"): (
        "Iron, steel and ferro-alloys",
        "Ferro-silico-aluminium, Fe-Si-Mn-Al, Fe-Si-Al-Ca",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-silico-chrome"): ("Iron, steel and ferro-alloys", "Ferro-silico-chrome"),
    ("Iron, steel and ferro-alloys", "Ferro-silico-magnesium"): (
        "Iron, steel and ferro-alloys",
        "Ferro-silico-magnesium",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-silico-manganese"): (
        "Iron, steel and ferro-alloys",
        "Ferro-silico-manganese",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-silico-manganese-aluminium"): (
        "Iron, steel and ferro-alloys",
        "Ferro-silico-manganese-aluminium",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-silico-zirconium"): (
        "Iron, steel and ferro-alloys",
        "Ferro-silico-zirconium",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-silicon"): ("Iron, steel and ferro-alloys", "Ferro-silicon"),
    ("Iron, steel and ferro-alloys", "Ferro-titanium"): ("Iron, steel and ferro-alloys", "Ferro-titanium"),
    ("Iron, steel and ferro-alloys", "Ferro-titanium & Fe-Si-Ti"): (
        "Iron, steel and ferro-alloys",
        "Ferro-titanium & Fe-Si-Ti",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-tungsten"): ("Iron, steel and ferro-alloys", "Ferro-tungsten"),
    ("Iron, steel and ferro-alloys", "Ferro-tungsten & Fe-Si-W"): (
        "Iron, steel and ferro-alloys",
        "Ferro-tungsten & Fe-Si-W",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-vanadium"): ("Iron, steel and ferro-alloys", "Ferro-vanadium"),
    ("Iron, steel and ferro-alloys", "Ferro-zirconium"): ("Iron, steel and ferro-alloys", "Ferro-zirconium"),
    ("Iron, steel and ferro-alloys", "Ingots, blooms, billets"): (
        "Iron, steel and ferro-alloys",
        "Ingots, blooms, billets",
    ),
    ("Iron, steel and ferro-alloys", "Other ferro-alloys"): ("Iron, steel and ferro-alloys", "Other ferro-alloys"),
    ("Iron, steel and ferro-alloys", "Pig iron"): ("Iron, steel and ferro-alloys", "Pig iron"),
    ("Iron, steel and ferro-alloys", "Pig iron & ferro-alloys"): (
        "Iron, steel and ferro-alloys",
        "Pig iron & ferro-alloys",
    ),
    ("Iron, steel and ferro-alloys", "Pig iron & ingots"): ("Iron, steel and ferro-alloys", "Pig iron & ingots"),
    ("Iron, steel and ferro-alloys", "Pig iron & spiegeleisen"): (
        "Iron, steel and ferro-alloys",
        "Pig iron & spiegeleisen",
    ),
    ("Iron, steel and ferro-alloys", "Pig iron & sponge"): ("Iron, steel and ferro-alloys", "Pig iron & sponge"),
    ("Iron, steel and ferro-alloys", "Powder"): ("Iron, steel and ferro-alloys", "Powder"),
    ("Iron, steel and ferro-alloys", "Scrap"): ("Iron, steel and ferro-alloys", "Scrap"),
    ("Iron, steel and ferro-alloys", "Shot, powder, sponge, etc."): (
        "Iron, steel and ferro-alloys",
        "Shot, powder, sponge, etc.",
    ),
    ("Iron, steel and ferro-alloys", "Silicon metal"): ("Iron, steel and ferro-alloys", "Silicon metal"),
    ("Iron, steel and ferro-alloys", "Spiegeleisen"): ("Iron, steel and ferro-alloys", "Spiegeleisen"),
    ("Iron, steel and ferro-alloys", "Sponge"): ("Iron, steel and ferro-alloys", "Sponge"),
    ("Iron, steel and ferro-alloys", "Sponge & powder"): ("Iron, steel and ferro-alloys", "Sponge & powder"),
    ("Iron, steel and ferro-alloys", "Tin-plate scrap"): ("Iron, steel and ferro-alloys", "Tin-plate scrap"),
    ("Kaolin", "Total"): ("Kaolin", "Total"),
    ("Lead", "Ores & concentrates"): ("Lead", "Ores & concentrates"),
    ("Lead", "Refined"): ("Lead", "Refined"),
    ("Lead", "Scrap"): ("Lead", "Scrap"),
    ("Lead", "Unwrought"): ("Lead", "Unwrought"),
    ("Lead", "Unwrought & scrap"): ("Lead", "Unwrought & scrap"),
    ("Lead", "Unwrought & semi-manufactures"): ("Lead", "Unwrought & semi-manufactures"),
    ("Lead", "Unwrought alloys"): ("Lead", "Unwrought alloys"),
    ("Lead, mine", "Total"): ("Lead, mine", "Total"),
    ("Lead, refined", "Total"): ("Lead, refined", "Total"),
    ("Lithium", "Carbonate"): ("Lithium", "Carbonate"),
    ("Lithium", "Lithium minerals"): ("Lithium", "Lithium minerals"),
    ("Lithium", "Lithium minerals, compounds & metal"): ("Lithium", "Lithium minerals, compounds & metal"),
    ("Lithium", "Metal"): ("Lithium", "Metal"),
    ("Lithium", "Oxides"): ("Lithium", "Oxides"),
    ("Lithium minerals", "Amblygonite"): ("Lithium minerals", "Amblygonite"),
    ("Lithium minerals", "Carbonate"): ("Lithium minerals", "Carbonate"),
    ("Lithium minerals", "Chloride"): ("Lithium minerals", "Chloride"),
    ("Lithium minerals", "Lepidolite"): ("Lithium minerals", "Lepidolite"),
    ("Lithium minerals", "Lithium minerals (Carbonate -Li content)"): (
        "Lithium minerals",
        "Lithium minerals (Carbonate -Li content)",
    ),
    ("Lithium minerals", "Lithium minerals (Chloride -Li content)"): (
        "Lithium minerals",
        "Lithium minerals (Chloride -Li content)",
    ),
    ("Lithium minerals", "Lithium minerals (Li content)"): ("Lithium minerals", "Lithium minerals (Li content)"),
    ("Lithium minerals", "Lithium minerals (hydroxide)"): ("Lithium minerals", "Lithium minerals (hydroxide)"),
    ("Lithium minerals", "Petalite"): ("Lithium minerals", "Petalite"),
    ("Lithium minerals", "Spodumene"): ("Lithium minerals", "Spodumene"),
    ("Lithium minerals", "Total"): ("Lithium minerals", "Total"),
    ("Magnesite", "Total"): ("Magnesite", "Total"),
    ("Magnesite and magnesia", "Magnesia"): ("Magnesite and magnesia", "Magnesia"),
    ("Magnesite and magnesia", "Magnesite"): ("Magnesite and magnesia", "Magnesite"),
    ("Magnesite and magnesia", "Magnesite, calcined"): ("Magnesite and magnesia", "Magnesite, calcined"),
    ("Magnesite and magnesia", "Magnesite, crude"): ("Magnesite and magnesia", "Magnesite, crude"),
    ("Magnesite and magnesia", "Magnesite, crude & calcined"): (
        "Magnesite and magnesia",
        "Magnesite, crude & calcined",
    ),
    ("Magnesite and magnesia", "Total"): ("Magnesite and magnesia", "Total"),
    ("Magnesium metal, primary", "Total"): ("Magnesium metal, primary", "Total"),
    ("Manganese", "Metal"): ("Manganese", "Metal"),
    ("Manganese", "Ores & Concentrates"): ("Manganese", "Ores & Concentrates"),
    ("Manganese ore", "Chemical"): ("Manganese ore", "Chemical"),
    ("Manganese ore", "Manganese ore (ferruginous)"): ("Manganese ore", "Manganese ore (ferruginous)"),
    ("Manganese ore", "Metallurgical"): ("Manganese ore", "Metallurgical"),
    ("Manganese ore", "Total"): ("Manganese ore", "Total"),
    ("Mercury", "Total"): ("Mercury", "Total"),
    ("Mica", "Block"): ("Mica", "Block"),
    ("Mica", "Condenser films"): ("Mica", "Condenser films"),
    ("Mica", "Crude"): ("Mica", "Crude"),
    ("Mica", "Ground"): ("Mica", "Ground"),
    ("Mica", "Ground & waste"): ("Mica", "Ground & waste"),
    ("Mica", "Mica"): ("Mica", "Mica"),
    ("Mica", "Other unmanufactured"): ("Mica", "Other unmanufactured"),
    ("Mica", "Phlogopite"): ("Mica", "Phlogopite"),
    ("Mica", "Sheet"): ("Mica", "Sheet"),
    ("Mica", "Splittings"): ("Mica", "Splittings"),
    ("Mica", "Total"): ("Mica", "Total"),
    ("Mica", "Unmanufactured"): ("Mica", "Unmanufactured"),
    ("Mica", "Waste"): ("Mica", "Waste"),
    ("Molybdenum", "Metal"): ("Molybdenum", "Metal"),
    ("Molybdenum", "Ores & concentrates"): ("Molybdenum", "Ores & concentrates"),
    ("Molybdenum", "Oxides"): ("Molybdenum", "Oxides"),
    ("Molybdenum", "Scrap"): ("Molybdenum", "Scrap"),
    ("Molybdenum, mine", "Total"): ("Molybdenum, mine", "Total"),
    ("Natural gas", "Total"): ("Natural gas", "Total"),
    ("Nepheline syenite", "Nepheline concentrates"): ("Nepheline syenite", "Nepheline concentrates"),
    ("Nepheline syenite", "Nepheline-syenite"): ("Nepheline syenite", "Nepheline-syenite"),
    ("Nepheline syenite", "Total"): ("Nepheline syenite", "Total"),
    ("Nickel", "Mattes, sinters etc"): ("Nickel", "Mattes, sinters etc"),
    ("Nickel", "Ores & concentrates"): ("Nickel", "Ores & concentrates"),
    ("Nickel", "Ores, concentrates & scrap"): ("Nickel", "Ores, concentrates & scrap"),
    ("Nickel", "Ores, concentrates, mattes etc"): ("Nickel", "Ores, concentrates, mattes etc"),
    ("Nickel", "Oxide, sinter & sulfide"): ("Nickel", "Oxide, sinter & sulfide"),
    ("Nickel", "Oxides"): ("Nickel", "Oxides"),
    ("Nickel", "Scrap"): ("Nickel", "Scrap"),
    ("Nickel", "Slurry, mattes, sinters etc"): ("Nickel", "Slurry, mattes, sinters etc"),
    ("Nickel", "Sulfide"): ("Nickel", "Sulfide"),
    ("Nickel", "Total"): ("Nickel", "Total"),
    ("Nickel", "Unwrought"): ("Nickel", "Unwrought"),
    ("Nickel", "Unwrought alloys"): ("Nickel", "Unwrought alloys"),
    ("Nickel", "Unwrought, mattes, sinters etc"): ("Nickel", "Unwrought, mattes, sinters etc"),
    ("Nickel, mine", "Total"): ("Nickel, mine", "Total"),
    ("Nickel, smelter/refinery", "Sulfate"): ("Nickel, smelter/refinery", "Sulfate"),
    ("Nickel, smelter/refinery", "Total"): ("Nickel, smelter/refinery", "Total"),
    ("Perlite", "Total"): ("Perlite", "Total"),
    ("Petroleum, crude", "Total"): ("Petroleum, crude", "Total"),
    ("Phosphate rock", "Aluminium phosphate"): ("Phosphate rock", "Aluminium phosphate"),
    ("Phosphate rock", "Apatite"): ("Phosphate rock", "Apatite"),
    ("Phosphate rock", "Calcium phosphates"): ("Phosphate rock", "Calcium phosphates"),
    ("Phosphate rock", "Guano"): ("Phosphate rock", "Guano"),
    ("Phosphate rock", "Total"): ("Phosphate rock", "Total"),
    ("Platinum group metals", "Iridium, osmium & ruthenium"): ("Platinum group metals", "Iridium, osmium & ruthenium"),
    ("Platinum group metals", "Ores & concentrates"): ("Platinum group metals", "Ores & concentrates"),
    ("Platinum group metals", "Other platinum group metals"): ("Platinum group metals", "Other platinum group metals"),
    ("Platinum group metals", "Other platinum metals"): ("Platinum group metals", "Other platinum metals"),
    ("Platinum group metals", "Palladium"): ("Platinum group metals", "Palladium"),
    ("Platinum group metals", "Platinum"): ("Platinum group metals", "Platinum"),
    ("Platinum group metals", "Platinum & platinum metals"): ("Platinum group metals", "Platinum & platinum metals"),
    ("Platinum group metals", "Platinum metals"): ("Platinum group metals", "Platinum metals"),
    ("Platinum group metals", "Rhodium"): ("Platinum group metals", "Rhodium"),
    ("Platinum group metals", "Ruthenium"): ("Platinum group metals", "Ruthenium"),
    ("Platinum group metals", "Sponge"): ("Platinum group metals", "Sponge"),
    ("Platinum group metals", "Waste & scrap"): ("Platinum group metals", "Waste & scrap"),
    ("Platinum group metals, mine", "Iridium"): ("Platinum group metals, mine", "Iridium"),
    ("Platinum group metals, mine", "Osmiridium"): ("Platinum group metals, mine", "Osmiridium"),
    ("Platinum group metals, mine", "Osmium"): ("Platinum group metals, mine", "Osmium"),
    ("Platinum group metals, mine", "Other platinum metals"): ("Platinum group metals, mine", "Other platinum metals"),
    ("Platinum group metals, mine", "Palladium"): ("Platinum group metals, mine", "Palladium"),
    ("Platinum group metals, mine", "Platinum"): ("Platinum group metals, mine", "Platinum"),
    ("Platinum group metals, mine", "Rhodium"): ("Platinum group metals, mine", "Rhodium"),
    ("Platinum group metals, mine", "Ruthenium"): ("Platinum group metals, mine", "Ruthenium"),
    ("Platinum group metals, mine", "Total"): ("Platinum group metals, mine", "Total"),
    ("Potash", "Carbonate"): ("Potash", "Carbonate"),
    ("Potash", "Caustic potash"): ("Potash", "Caustic potash"),
    ("Potash", "Chlorate"): ("Potash", "Chlorate"),
    ("Potash", "Chloride"): ("Potash", "Chloride"),
    ("Potash", "Cyanide"): ("Potash", "Cyanide"),
    ("Potash", "Fertiliser salts"): ("Potash", "Fertiliser salts"),
    ("Potash", "Kainite, sylvinite"): ("Potash", "Kainite, sylvinite"),
    ("Potash", "Nitrate"): ("Potash", "Nitrate"),
    ("Potash", "Other fertiliser salts"): ("Potash", "Other fertiliser salts"),
    ("Potash", "Other potassic chemicals"): ("Potash", "Other potassic chemicals"),
    ("Potash", "Other potassic fertilisers"): ("Potash", "Other potassic fertilisers"),
    ("Potash", "Polyhalite"): ("Potash", "Polyhalite"),
    ("Potash", "Potassic chemicals"): ("Potash", "Potassic chemicals"),
    ("Potash", "Potassic fertilisers"): ("Potash", "Potassic fertilisers"),
    ("Potash", "Potassic salts"): ("Potash", "Potassic salts"),
    ("Potash", "Sulfide"): ("Potash", "Sulfide"),
    ("Potash", "Total"): ("Potash", "Total"),
    ("Rare earth minerals", "Bastnaesite"): ("Rare earth minerals", "Bastnaesite"),
    ("Rare earth minerals", "Loparite"): ("Rare earth minerals", "Loparite"),
    ("Rare earth minerals", "Monazite"): ("Rare earth minerals", "Monazite"),
    ("Rare earth minerals", "Total"): ("Rare earth minerals", "Total"),
    ("Rare earth minerals", "Xenotime"): ("Rare earth minerals", "Xenotime"),
    ("Rare earth oxides", "Total"): ("Rare earth oxides", "Total"),
    ("Rare earths", "Cerium compounds"): ("Rare earths", "Cerium compounds"),
    ("Rare earths", "Cerium metal"): ("Rare earths", "Cerium metal"),
    ("Rare earths", "Ferro-cerium & other pyrophoric alloys"): (
        "Rare earths",
        "Ferro-cerium & other pyrophoric alloys",
    ),
    ("Rare earths", "Metals"): ("Rare earths", "Metals"),
    ("Rare earths", "Ores & concentrates"): ("Rare earths", "Ores & concentrates"),
    ("Rare earths", "Other rare earth compounds"): ("Rare earths", "Other rare earth compounds"),
    ("Rare earths", "Rare earth compounds"): ("Rare earths", "Rare earth compounds"),
    ("Rhenium", "Total"): ("Rhenium", "Total"),
    ("Salt", "Brine salt"): ("Salt", "Brine salt"),
    ("Salt", "Brine salt & sea salt"): ("Salt", "Brine salt & sea salt"),
    ("Salt", "Common salt"): ("Salt", "Common salt"),
    ("Salt", "Crude salt"): ("Salt", "Crude salt"),
    ("Salt", "Evaporated salt"): ("Salt", "Evaporated salt"),
    ("Salt", "Other salt"): ("Salt", "Other salt"),
    ("Salt", "Refined salt"): ("Salt", "Refined salt"),
    ("Salt", "Rock salt"): ("Salt", "Rock salt"),
    ("Salt", "Rock salt & brine salt"): ("Salt", "Rock salt & brine salt"),
    ("Salt", "Salt in brine"): ("Salt", "Salt in brine"),
    ("Salt", "Sea salt"): ("Salt", "Sea salt"),
    ("Salt", "Total"): ("Salt", "Total"),
    ("Selenium, refined", "Total"): ("Selenium, refined", "Total"),
    ("Sillimanite minerals", "Andalusite"): ("Sillimanite minerals", "Andalusite"),
    ("Sillimanite minerals", "Andalusite & kyanite"): ("Sillimanite minerals", "Andalusite & kyanite"),
    ("Sillimanite minerals", "Kyanite"): ("Sillimanite minerals", "Kyanite"),
    ("Sillimanite minerals", "Kyanite & related minerals"): ("Sillimanite minerals", "Kyanite & related minerals"),
    ("Sillimanite minerals", "Mullite"): ("Sillimanite minerals", "Mullite"),
    ("Sillimanite minerals", "Mullite, chamotte, dinas earth"): (
        "Sillimanite minerals",
        "Mullite, chamotte, dinas earth",
    ),
    ("Sillimanite minerals", "Other"): ("Sillimanite minerals", "Other"),
    ("Sillimanite minerals", "Sillimanite"): ("Sillimanite minerals", "Sillimanite"),
    ("Sillimanite minerals", "Sillimanite minerals"): ("Sillimanite minerals", "Sillimanite minerals"),
    ("Sillimanite minerals", "Sillimanite minerals & dinas earth"): (
        "Sillimanite minerals",
        "Sillimanite minerals & dinas earth",
    ),
    ("Sillimanite minerals", "Sillimanite minerals, calcined"): (
        "Sillimanite minerals",
        "Sillimanite minerals, calcined",
    ),
    ("Sillimanite minerals", "Sillimanite minerals, crude"): ("Sillimanite minerals", "Sillimanite minerals, crude"),
    ("Sillimanite minerals", "Sillimanite minerals, crude & calcined"): (
        "Sillimanite minerals",
        "Sillimanite minerals, crude & calcined",
    ),
    ("Sillimanite minerals", "Sillimanite minerals, other"): ("Sillimanite minerals", "Sillimanite minerals, other"),
    ("Sillimanite minerals", "Sillimanite mins, chamotte, dinas earth"): (
        "Sillimanite minerals",
        "Sillimanite mins, chamotte, dinas earth",
    ),
    ("Sillimanite minerals", "Total"): ("Sillimanite minerals", "Total"),
    ("Silver", "Alloys"): ("Silver", "Alloys"),
    ("Silver", "Metal"): ("Silver", "Metal"),
    ("Silver", "Metal, refined"): ("Silver", "Metal, refined"),
    ("Silver", "Metal, unrefined"): ("Silver", "Metal, unrefined"),
    ("Silver", "Ores & concentrates"): ("Silver", "Ores & concentrates"),
    ("Silver", "Silver-lead bullion"): ("Silver", "Silver-lead bullion"),
    ("Silver", "Total"): ("Silver", "Total"),
    ("Silver", "Waste & scrap"): ("Silver", "Waste & scrap"),
    ("Silver, mine", "Total"): ("Silver, mine", "Total"),
    ("Sodium carbonate, natural", "Total"): ("Sodium carbonate, natural", "Total"),
    ("Steel, crude", "Total"): ("Steel, crude", "Total"),
    ("Strontium minerals", "Total"): ("Strontium minerals", "Total"),
    ("Sulphur and pyrites", "Other"): ("Sulphur and pyrites", "Other"),
    ("Sulphur and pyrites", "Precipitated"): ("Sulphur and pyrites", "Precipitated"),
    ("Sulphur and pyrites", "Pyrites"): ("Sulphur and pyrites", "Pyrites"),
    ("Sulphur and pyrites", "Pyrites - cupreous"): ("Sulphur and pyrites", "Pyrites - cupreous"),
    ("Sulphur and pyrites", "Pyrites - iron"): ("Sulphur and pyrites", "Pyrites - iron"),
    ("Sulphur and pyrites", "Sublimed"): ("Sulphur and pyrites", "Sublimed"),
    ("Sulphur and pyrites", "Sulfur"): ("Sulphur and pyrites", "Sulfur"),
    ("Sulphur and pyrites", "Sulfur, crude"): ("Sulphur and pyrites", "Sulfur, crude"),
    ("Sulphur and pyrites", "Sulfur, other"): ("Sulphur and pyrites", "Sulfur, other"),
    ("Sulphur and pyrites", "Sulfur, refined"): ("Sulphur and pyrites", "Sulfur, refined"),
    ("Sulphur and pyrites", "Sulfur, sublimed & precipitated"): (
        "Sulphur and pyrites",
        "Sulfur, sublimed & precipitated",
    ),
    ("Sulphur and pyrites", "Zinc concentrates"): ("Sulphur and pyrites", "Zinc concentrates"),
    ("Talc", "Agalmatolite"): ("Talc", "Agalmatolite"),
    ("Talc", "Other"): ("Talc", "Other"),
    ("Talc", "Pyrophyllite"): ("Talc", "Pyrophyllite"),
    ("Talc", "Steatite"): ("Talc", "Steatite"),
    ("Talc", "Total"): ("Talc", "Total"),
    ("Talc", "Wonderstone (Pyrophyllite)"): ("Talc", "Wonderstone (Pyrophyllite)"),
    ("Tantalum and niobium", "Columbite"): ("Tantalum and niobium", "Columbite"),
    ("Tantalum and niobium", "Columbite & tantalite"): ("Tantalum and niobium", "Columbite & tantalite"),
    ("Tantalum and niobium", "Columbite-tantalite"): ("Tantalum and niobium", "Columbite-tantalite"),
    ("Tantalum and niobium", "Niobium"): ("Tantalum and niobium", "Niobium"),
    ("Tantalum and niobium", "Niobium & tantalum ores"): ("Tantalum and niobium", "Niobium & tantalum ores"),
    ("Tantalum and niobium", "Niobium concentrates"): ("Tantalum and niobium", "Niobium concentrates"),
    ("Tantalum and niobium", "Niobium ores & concentrates"): ("Tantalum and niobium", "Niobium ores & concentrates"),
    ("Tantalum and niobium", "Pyrochlore"): ("Tantalum and niobium", "Pyrochlore"),
    ("Tantalum and niobium", "Tantalum"): ("Tantalum and niobium", "Tantalum"),
    ("Tantalum and niobium", "Tantalum concentrates"): ("Tantalum and niobium", "Tantalum concentrates"),
    ("Tantalum and niobium", "Tantalum ores & concentrates"): ("Tantalum and niobium", "Tantalum ores & concentrates"),
    ("Tantalum and niobium", "Tin slags"): ("Tantalum and niobium", "Tin slags"),
    ("Tantalum and niobium", "Tin slags, Nb content"): ("Tantalum and niobium", "Tin slags, Nb content"),
    ("Tantalum and niobium", "Tin slags, Ta content"): ("Tantalum and niobium", "Tin slags, Ta content"),
    ("Tantalum and niobium", "Total"): ("Tantalum and niobium", "Total"),
    ("Tantalum and niobium minerals", "Columbite"): ("Tantalum and niobium minerals", "Columbite"),
    ("Tantalum and niobium minerals", "Columbite- Nb content"): (
        "Tantalum and niobium minerals",
        "Columbite- Nb content",
    ),
    ("Tantalum and niobium minerals", "Columbite- Ta content"): (
        "Tantalum and niobium minerals",
        "Columbite- Ta content",
    ),
    ("Tantalum and niobium minerals", "Columbite-tantalite"): ("Tantalum and niobium minerals", "Columbite-tantalite"),
    ("Tantalum and niobium minerals", "Columbite-tantalite-Nb content"): (
        "Tantalum and niobium minerals",
        "Columbite-tantalite-Nb content",
    ),
    ("Tantalum and niobium minerals", "Columbite-tantalite-Ta content"): (
        "Tantalum and niobium minerals",
        "Columbite-tantalite-Ta content",
    ),
    ("Tantalum and niobium minerals", "Djalmaite"): ("Tantalum and niobium minerals", "Djalmaite"),
    ("Tantalum and niobium minerals", "Microlite"): ("Tantalum and niobium minerals", "Microlite"),
    ("Tantalum and niobium minerals", "Pyrochlore"): ("Tantalum and niobium minerals", "Pyrochlore"),
    ("Tantalum and niobium minerals", "Pyrochlore -Nb content"): (
        "Tantalum and niobium minerals",
        "Pyrochlore -Nb content",
    ),
    ("Tantalum and niobium minerals", "Struverite"): ("Tantalum and niobium minerals", "Struverite"),
    ("Tantalum and niobium minerals", "Struverite (Ta content)"): (
        "Tantalum and niobium minerals",
        "Struverite (Ta content)",
    ),
    ("Tantalum and niobium minerals", "Tantalite"): ("Tantalum and niobium minerals", "Tantalite"),
    ("Tantalum and niobium minerals", "Tantalite -Ta content"): (
        "Tantalum and niobium minerals",
        "Tantalite -Ta content",
    ),
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Nb content)"): (
        "Tantalum and niobium minerals",
        "Tantalum & Niobium (Nb content)",
    ),
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Ta content)"): (
        "Tantalum and niobium minerals",
        "Tantalum & Niobium (Ta content)",
    ),
    ("Tellurium, refined", "Total"): ("Tellurium, refined", "Total"),
    ("Tin", "Concentrates"): ("Tin", "Concentrates"),
    ("Tin", "Scrap"): ("Tin", "Scrap"),
    ("Tin", "Tin-silver ore"): ("Tin", "Tin-silver ore"),
    ("Tin", "Unwrought"): ("Tin", "Unwrought"),
    ("Tin", "Unwrought & scrap"): ("Tin", "Unwrought & scrap"),
    ("Tin", "Unwrought & semi-manufactures"): ("Tin", "Unwrought & semi-manufactures"),
    ("Tin", "Unwrought alloys"): ("Tin", "Unwrought alloys"),
    ("Tin, mine", "Total"): ("Tin, mine", "Total"),
    ("Tin, smelter", "Total"): ("Tin, smelter", "Total"),
    ("Titanium", "Ilmenite"): ("Titanium", "Ilmenite"),
    ("Titanium", "Leucoxene"): ("Titanium", "Leucoxene"),
    ("Titanium", "Metal"): ("Titanium", "Metal"),
    ("Titanium", "Ores & concentrates"): ("Titanium", "Ores & concentrates"),
    ("Titanium", "Other titanium minerals"): ("Titanium", "Other titanium minerals"),
    ("Titanium", "Oxides"): ("Titanium", "Oxides"),
    ("Titanium", "Rutile"): ("Titanium", "Rutile"),
    ("Titanium", "Rutile sand"): ("Titanium", "Rutile sand"),
    ("Titanium", "Sponge"): ("Titanium", "Sponge"),
    ("Titanium", "Titanium minerals"): ("Titanium", "Titanium minerals"),
    ("Titanium", "Titanium minerals & slag"): ("Titanium", "Titanium minerals & slag"),
    ("Titanium", "Titanium slag"): ("Titanium", "Titanium slag"),
    ("Titanium", "Unwrought"): ("Titanium", "Unwrought"),
    ("Titanium", "Wrought"): ("Titanium", "Wrought"),
    ("Titanium minerals", "Ilmenite"): ("Titanium minerals", "Ilmenite"),
    ("Titanium minerals", "Leucoxene"): ("Titanium minerals", "Leucoxene"),
    ("Titanium minerals", "Rutile"): ("Titanium minerals", "Rutile"),
    ("Titanium minerals", "Titanium slag"): ("Titanium minerals", "Titanium slag"),
    ("Titanium minerals", "Total"): ("Titanium minerals", "Total"),
    ("Tungsten", "Ammonium paratungstate"): ("Tungsten", "Ammonium paratungstate"),
    ("Tungsten", "Carbide"): ("Tungsten", "Carbide"),
    ("Tungsten", "Metal"): ("Tungsten", "Metal"),
    ("Tungsten", "Ores & concentrates"): ("Tungsten", "Ores & concentrates"),
    ("Tungsten", "Other tungsten ores"): ("Tungsten", "Other tungsten ores"),
    ("Tungsten", "Powder"): ("Tungsten", "Powder"),
    ("Tungsten", "Scheelite ores & concentrates"): ("Tungsten", "Scheelite ores & concentrates"),
    ("Tungsten", "Total"): ("Tungsten", "Total"),
    ("Tungsten", "Wolframite ores & concentrates"): ("Tungsten", "Wolframite ores & concentrates"),
    ("Tungsten, mine", "Total"): ("Tungsten, mine", "Total"),
    ("Uranium", "Total"): ("Uranium", "Total"),
    ("Vanadium", "Lead vanadium concentrates"): ("Vanadium", "Lead vanadium concentrates"),
    ("Vanadium", "Metal"): ("Vanadium", "Metal"),
    ("Vanadium", "Ores & concentrates"): ("Vanadium", "Ores & concentrates"),
    ("Vanadium", "Pentoxide"): ("Vanadium", "Pentoxide"),
    ("Vanadium", "Vanadiferous residues"): ("Vanadium", "Vanadiferous residues"),
    ("Vanadium", "Vanadium-titanium pig iron"): ("Vanadium", "Vanadium-titanium pig iron"),
    ("Vanadium, mine", "Total"): ("Vanadium, mine", "Total"),
    ("Vermiculite", "Total"): ("Vermiculite", "Total"),
    ("Wollastonite", "Total"): ("Wollastonite", "Total"),
    ("Zinc", "Crude & refined"): ("Zinc", "Crude & refined"),
    ("Zinc", "Ores & concentrates"): ("Zinc", "Ores & concentrates"),
    ("Zinc", "Oxides"): ("Zinc", "Oxides"),
    ("Zinc", "Scrap"): ("Zinc", "Scrap"),
    ("Zinc", "Unwrought"): ("Zinc", "Unwrought"),
    ("Zinc", "Unwrought alloys"): ("Zinc", "Unwrought alloys"),
    ("Zinc, mine", "Total"): ("Zinc, mine", "Total"),
    ("Zinc, slab", "Total"): ("Zinc, slab", "Total"),
    ("Zirconium", "Concentrates"): ("Zirconium", "Concentrates"),
    ("Zirconium", "Metal"): ("Zirconium", "Metal"),
    ("Zirconium", "Total"): ("Zirconium", "Total"),
    ("Zirconium", "Zirconium sand"): ("Zirconium", "Zirconium sand"),
    ("Zirconium minerals", "Total"): ("Zirconium minerals", "Total"),
}


def harmonize_commodity_subcommodity_pairs(tb: Table) -> Table:
    tb = tb.astype({"commodity": str, "sub_commodity": str}).copy()
    for pair_old, pair_new in COMMODITY_MAPPING.items():
        if pair_old == pair_new:
            # Nothing to do, continue.
            continue

        # Get the old commodity-subcommodity names.
        commodity_old, subcommodity_old = pair_old
        if pair_new is None:
            # Remove rows for this combination.
            index_to_drop = tb.loc[(tb["commodity"] == commodity_old) & (tb["sub_commodity"] == subcommodity_old)].index
            tb = tb.drop(index_to_drop).reset_index(drop=True)
            continue

        # Get the new commodity-subcommodity names.
        commodity_new, subcommodity_new = pair_new
        # Rename the commodity-subcommodity pair.
        tb.loc[
            (tb["commodity"] == commodity_old) & (tb["sub_commodity"] == subcommodity_old),
            ["commodity", "sub_commodity"],
        ] = pair_new

    return tb


def harmonize_units(tb: Table) -> Table:
    tb = tb.astype({"value": "Float64", "unit": "string"}).copy()
    units = sorted(set(tb["unit"]))

    # Mapping from original unit names to tonnes.
    mapping = {
        "tonnes": "tonnes",
        "tonnes (metric)": "tonnes",
        "tonnes (Al2O3 content)": "tonnes of Al2O3 content",
        "tonnes (K20 content)": "tonnes of K₂O content",
        "tonnes (metal content)": "tonnes of metal content",
        "kilograms": "tonnes",
        "kilograms (metal content)": "tonnes of metal content",
        "Carats": "tonnes",
        "million cubic metres": "tonnes",
    }

    # Sanity check.
    error = "Unexpected units found. Add them to the unit mapping and decide its conversion."
    assert set(units) == set(mapping), error

    for unit in units:
        mask = tb["unit"] == unit
        if unit in [
            "tonnes",
            "tonnes (metal content)",
            "tonnes (Al2O3 content)",
            "tonnes (K20 content)",
            "tonnes (metric)",
        ]:
            pass
        elif unit in ["kilograms", "kilograms (metal content)"]:
            tb.loc[mask, "value"] *= 1e-3
        elif unit in ["Carats"]:
            tb.loc[mask, "value"] *= CARATS_TO_TONNES
        elif unit in ["million cubic metres"]:
            # TODO: Assert that commodity is either helium or natural gas, and convert accordingly.
            error = "Unexpected commodity using million cubic metres."
            assert set(tb[mask]["commodity"]) == {"helium", "natural gas"}, error
            tb.loc[mask & (tb["commodity"] == "helium"), "value"] *= MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES
            tb.loc[mask & (tb["commodity"] == "natural gas"), "value"] *= MILLION_CUBIC_METERS_OF_NATURAL_GAS_TO_TONNES
        tb.loc[mask, "unit"] = mapping[unit]

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("world_mineral_statistics")
    tb = ds_meadow.read_table("world_mineral_statistics")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Harmonize units.
    tb = harmonize_units(tb=tb)

    # Improve the name of the commodities.
    tb["commodity"] = tb["commodity"].str.capitalize()

    # Harmonize commodity-subcommodity pairs.
    tb = harmonize_commodity_subcommodity_pairs(tb=tb)

    # Pivot table to have a column for each category.
    tb = tb.pivot(
        index=["country", "year", "commodity", "sub_commodity", "unit"],
        columns="category",
        values="value",
        join_column_levels_with="_",
    )

    # TODO: There are many issues to be handled:
    #  * There are many overlapping historical regions. For example, Diatomite has production from USSR until 2006!

    # Add regions and income groups to the table.
    REGIONS = {**geo.REGIONS, **{"World": {}}}
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        index_columns=["country", "year", "commodity", "sub_commodity"],
    )

    # Format table conveniently.
    # NOTE: All commodities have the same unit for imports, exports and production except one:
    #  Potash Chloride uses "tonnes" for imports and exports, and "tonnes of K20 content" (which is also misspelled).
    #  Due to this, the index cannot simply be "country", "year", "commodity", "sub_commodity"; we need also "unit".
    # counts = tb.groupby(["commodity", "sub_commodity", "country", "year"], observed=True, as_index=False).nunique()
    # counts[counts["unit"] > 1][["commodity", "sub_commodity"]].drop_duplicates()
    tb = tb.format(["country", "year", "commodity", "sub_commodity", "unit"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
