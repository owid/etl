"""Load a meadow dataset and create a garden dataset."""

import ast
from typing import Dict, List

import pandas as pd
from owid.catalog import Table, VariablePresentationMeta
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Convert carats to metric tonnes.
CARATS_TO_TONNES = 2e-7
# Convert million cubic meters of helium to metric tonnes.
MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES = 178.5
# Convert million cubic meters of natural gas to metric tonnes.
# NOTE: This conversion is irrelevant, since natural gas is removed (see notes below in COMMODITY_MAPPING).
MILLION_CUBIC_METERS_OF_NATURAL_GAS_TO_TONNES = 800

# Harmonize commodity-subcommodity names.
# NOTE: This list should contain all commodity-subcommodity pairs expected in the data.
# Set to None any commodity-subcommodity that should not be included.
COMMODITY_MAPPING = {
    # NOTE: When comparing with USGS' crushed stone, the numbers are very different (USGS' data for the US is larger than BGS' data for the World).
    ("Aggregates, primary", "Crushed rock"): None,
    # NOTE: Data for construction sand and gravel for BGS and USGS are very different.
    ("Aggregates, primary", "Sand and gravel"): None,
    ("Aggregates, primary", "Unknown"): None,
    ("Alumina", "Unknown"): ("Alumina", "Unknown"),
    ("Aluminium, primary", "Unknown"): ("Aluminum", "Primary"),
    ("Antimony", "Crude"): ("Antimony", "Crude"),
    ("Antimony", "Crude & regulus"): ("Antimony", "Crude & regulus"),
    ("Antimony", "Liquated"): ("Antimony", "Liquated"),
    ("Antimony", "Metal"): ("Antimony", "Metal"),
    ("Antimony", "Ores & concentrates"): ("Antimony", "Ores & concentrates"),
    ("Antimony", "Oxide"): ("Antimony", "Oxide"),
    ("Antimony", "Refined & regulus"): ("Antimony", "Refined & regulus"),
    ("Antimony", "Regulus"): ("Antimony", "Regulus"),
    ("Antimony", "Sulfide"): ("Antimony", "Sulfide"),
    ("Antimony, mine", "Unknown"): ("Antimony", "Mine"),
    ("Arsenic", "Metallic arsenic"): ("Arsenic", "Metallic arsenic"),
    ("Arsenic", "Unknown"): ("Arsenic", "Unknown"),
    ("Arsenic", "White arsenic"): ("Arsenic", "White arsenic"),
    ("Arsenic, white", "Unknown"): ("Arsenic", "White arsenic"),
    ("Asbestos", "Amosite"): ("Asbestos", "Amosite"),
    ("Asbestos", "Amphibole"): ("Asbestos", "Amphibole"),
    ("Asbestos", "Anthophyllite"): ("Asbestos", "Anthophyllite"),
    ("Asbestos", "Chrysotile"): ("Asbestos", "Chrysotile"),
    ("Asbestos", "Crocidolite"): ("Asbestos", "Crocidolite"),
    ("Asbestos", "Unknown"): ("Asbestos", "Unknown"),
    ("Asbestos, unmanufactured", "Amosite"): ("Asbestos", "Amosite, unmanufactured"),
    ("Asbestos, unmanufactured", "Chrysotile"): ("Asbestos", "Chrysotile, unmanufactured"),
    ("Asbestos, unmanufactured", "Crocidolite"): ("Asbestos", "Crocidolite, unmanufactured"),
    # NOTE: In the original BGS data, there is "Asbestos, unmanufactured" with subcommodity "Other manufactured".
    #  It's unclear what this means, but I'll assume that it's a mistake and that it should be other unmanufactured.
    #  This happens, e.g. to USA 1986 imports.
    ("Asbestos, unmanufactured", "Other manufactured"): ("Asbestos", "Other, unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured"): ("Asbestos", "Total, unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured, crude"): ("Asbestos", "Crude, unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured, fibre"): ("Asbestos", "Fibre, unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured, shorts"): ("Asbestos", "Shorts, unmanufactured"),
    ("Asbestos, unmanufactured", "Unmanufactured, waste"): ("Asbestos", "Waste, unmanufactured"),
    ("Asbestos, unmanufactured", "Waste"): ("Asbestos", "Waste, unmanufactured"),
    ("Barytes", "Barium minerals"): ("Barite", "Barium minerals"),
    ("Barytes", "Barytes"): ("Barite", "Unknown"),
    ("Barytes", "Unknown"): ("Barite", "Unknown"),
    ("Barytes", "Witherite"): ("Barite", "Witherite"),
    ("Bauxite", "Unknown"): ("Bauxite", "Unknown"),
    ("Bauxite, alumina and aluminium", "Alumina"): ("Bauxite, alumina and aluminum", "Alumina"),
    ("Bauxite, alumina and aluminium", "Alumina hydrate"): ("Bauxite, alumina and aluminum", "Alumina hydrate"),
    ("Bauxite, alumina and aluminium", "Bauxite"): ("Bauxite, alumina and aluminum", "Bauxite"),
    ("Bauxite, alumina and aluminium", "Bauxite, calcined"): ("Bauxite, alumina and aluminum", "Bauxite, calcined"),
    ("Bauxite, alumina and aluminium", "Bauxite, crude dried"): (
        "Bauxite, alumina and aluminum",
        "Bauxite, crude dried",
    ),
    ("Bauxite, alumina and aluminium", "Bauxite, dried"): ("Bauxite, alumina and aluminum", "Bauxite, dried"),
    ("Bauxite, alumina and aluminium", "Bauxite, uncalcined"): (
        "Bauxite, alumina and aluminum",
        "Bauxite, uncalcined",
    ),
    ("Bauxite, alumina and aluminium", "Scrap"): ("Bauxite, alumina and aluminum", "Scrap"),
    ("Bauxite, alumina and aluminium", "Unwrought"): ("Bauxite, alumina and aluminum", "Unwrought"),
    ("Bauxite, alumina and aluminium", "Unwrought & scrap"): ("Bauxite, alumina and aluminum", "Unwrought & scrap"),
    ("Bauxite, alumina and aluminium", "Unwrought alloys"): ("Bauxite, alumina and aluminum", "Unwrought alloys"),
    ("Bentonite and fuller's earth", "Attapulgite"): ("Bentonite and fuller's earth", "Attapulgite"),
    ("Bentonite and fuller's earth", "Bentonite"): ("Bentonite and fuller's earth", "Bentonite"),
    ("Bentonite and fuller's earth", "Fuller's earth"): ("Bentonite and fuller's earth", "Fuller's earth"),
    ("Bentonite and fuller's earth", "Sepiolite"): ("Bentonite and fuller's earth", "Sepiolite"),
    ("Bentonite and fuller's earth", "Unknown"): ("Bentonite and fuller's earth", "Unknown"),
    ("Beryl", "Unknown"): ("Beryl", "Unknown"),
    ("Bismuth", "Compounds"): ("Bismuth", "Compounds"),
    ("Bismuth", "Metal"): ("Bismuth", "Metal"),
    ("Bismuth", "Ores & concentrates"): ("Bismuth", "Ores & concentrates"),
    ("Bismuth, mine", "Unknown"): ("Bismuth", "Mine"),
    ("Borates", "Unknown"): ("Borates", "Unknown"),
    ("Bromine", "Compounds"): ("Bromine", "Compounds"),
    ("Bromine", "Unknown"): ("Bromine", "Unknown"),
    ("Cadmium", "Metal"): ("Cadmium", "Refinery"),
    ("Cadmium", "Other"): None,
    ("Cadmium", "Oxide"): None,
    ("Cadmium", "Sulfide"): None,
    ("Cadmium", "Unknown"): None,
    ("Cement", "Cement clinkers"): None,
    ("Cement", "Other cement"): None,
    ("Cement", "Portland cement"): None,
    ("Cement  clinker", "Cement, clinker"): None,
    ("Cement, finished", "Cement, finished"): None,
    ("Chromium", "Metal"): ("Chromium", "Metal"),
    ("Chromium", "Ores & concentrates"): None,
    ("Chromium ores and concentrates", "Unknown"): None,
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
    ("Coal", "Unknown"): ("Coal", "Unknown"),
    ("Cobalt", "Metal & refined"): None,
    ("Cobalt", "Ore"): None,
    ("Cobalt", "Oxide, sinter & sulfide"): None,
    ("Cobalt", "Oxides"): None,
    ("Cobalt", "Salts"): None,
    ("Cobalt", "Scrap"): None,
    ("Cobalt", "Unknown"): None,
    ("Cobalt", "Unwrought"): None,
    ("Cobalt, mine", "Unknown"): ("Cobalt", "Mine"),
    ("Cobalt, refined", "Unknown"): ("Cobalt", "Refinery"),
    ("Copper", "Ash and residues"): None,
    ("Copper", "Burnt cupreous pyrites"): None,
    ("Copper", "Cement copper"): None,
    ("Copper", "Matte"): None,
    ("Copper", "Matte & Scrap"): None,
    ("Copper", "Matte & cement"): None,
    ("Copper", "Ore & matte"): None,
    ("Copper", "Ores & concentrates"): None,
    ("Copper", "Ores, concentrates & matte"): None,
    ("Copper", "Scrap"): None,
    ("Copper", "Sludge, slimes & residues"): None,
    ("Copper", "Unknown"): None,
    ("Copper", "Unwrought"): None,
    ("Copper", "Unwrought & Scrap"): None,
    ("Copper", "Unwrought alloys"): None,
    ("Copper", "Unwrought, matte, cement & refined"): None,
    ("Copper", "Unwrought, refined"): None,
    ("Copper", "Unwrought, unrefined"): None,
    ("Copper, mine", "Unknown"): ("Copper", "Mine"),
    ("Copper, refined", "Unknown"): ("Copper", "Refinery"),
    ("Copper, smelter", "Unknown"): ("Copper", "Smelter"),
    # NOTE: It's unclear when synthetic diamond is included.
    # Production does not seem to include it, but imports and exports of "Dust" does include synthetic diamond.
    ("Diamond", "Cut"): ("Diamond", "Cut"),
    ("Diamond", "Dust"): ("Diamond", "Dust"),
    ("Diamond", "Gem"): ("Diamond", "Gem"),
    ("Diamond", "Gem, cut"): ("Diamond", "Gem, cut"),
    ("Diamond", "Gem, rough"): ("Diamond", "Gem, rough"),
    ("Diamond", "Industrial"): ("Diamond", "Mine, industrial"),
    ("Diamond", "Other"): ("Diamond", "Other"),
    ("Diamond", "Rough"): ("Diamond", "Rough"),
    ("Diamond", "Rough & Cut"): ("Diamond", "Rough & Cut"),
    ("Diamond", "Unknown"): ("Diamond", "Unknown"),
    ("Diamond", "Unsorted"): ("Diamond", "Unsorted"),
    ("Diatomite", "Activated diatomite"): ("Diatomite", "Activated diatomite"),
    ("Diatomite", "Moler"): ("Diatomite", "Moler"),
    ("Diatomite", "Moler bricks"): ("Diatomite", "Moler bricks"),
    ("Diatomite", "Unknown"): ("Diatomite", "Unknown"),
    ("Feldspar", "Unknown"): ("Feldspar", "Unknown"),
    ("Ferro-alloys", "Calcium silicide"): None,
    ("Ferro-alloys", "Fe-Ti, Fe-W, Fe-Mo, Fe-V"): None,
    ("Ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"): None,
    ("Ferro-alloys", "Ferro-Alloys (Ferro-silicon & silicon metal)"): None,
    ("Ferro-alloys", "Ferro-Si-manganese & silico-speigeleisen"): None,
    ("Ferro-alloys", "Ferro-alloys"): None,
    ("Ferro-alloys", "Ferro-aluminium"): None,
    ("Ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): None,
    ("Ferro-alloys", "Ferro-calcium-silicon"): None,
    ("Ferro-alloys", "Ferro-chrome"): None,
    ("Ferro-alloys", "Ferro-chrome & ferro-silico-chrome"): None,
    ("Ferro-alloys", "Ferro-manganese"): None,
    ("Ferro-alloys", "Ferro-manganese & ferro-silico-manganese"): None,
    ("Ferro-alloys", "Ferro-manganese & spiegeleisen"): None,
    ("Ferro-alloys", "Ferro-molybdenum"): None,
    ("Ferro-alloys", "Ferro-nickel"): None,
    ("Ferro-alloys", "Ferro-niobium"): None,
    ("Ferro-alloys", "Ferro-phosphorus"): None,
    ("Ferro-alloys", "Ferro-rare earth"): None,
    ("Ferro-alloys", "Ferro-silico-calcium-aluminium"): None,
    ("Ferro-alloys", "Ferro-silico-chrome"): None,
    ("Ferro-alloys", "Ferro-silico-magnesium"): None,
    ("Ferro-alloys", "Ferro-silico-manganese"): None,
    ("Ferro-alloys", "Ferro-silicon"): None,
    ("Ferro-alloys", "Ferro-titanium"): None,
    ("Ferro-alloys", "Ferro-titanium & ferro-silico-titanium"): None,
    ("Ferro-alloys", "Ferro-tungsten"): None,
    ("Ferro-alloys", "Ferro-vanadium"): None,
    ("Ferro-alloys", "Other ferro-alloys"): None,
    ("Ferro-alloys", "Pig iron"): None,
    ("Ferro-alloys", "Silicon metal"): None,
    ("Ferro-alloys", "Silicon pig iron"): None,
    ("Ferro-alloys", "Spiegeleisen"): None,
    ("Fluorspar", "Unknown"): ("Fluorspar", "Unknown"),
    ("Gallium, primary", "Unknown"): ("Gallium", "Primary"),
    ("Gemstones", "Unknown"): ("Gemstones", "Unknown"),
    ("Germanium metal", "Unknown"): ("Germanium", "Metal"),
    ("Gold", "Metal"): ("Gold", "Metal"),
    ("Gold", "Metal, other"): None,
    ("Gold", "Metal, refined"): ("Gold", "Metal, refined"),
    ("Gold", "Metal, unrefined"): None,
    ("Gold", "Ores & concentrates"): ("Gold", "Ores & concentrates"),
    ("Gold", "Ores, concentrates & unrefined metal"): None,
    ("Gold", "Waste & scrap"): None,
    ("Gold, mine", "Unknown"): ("Gold", "Mine"),
    ("Graphite", "Unknown"): ("Graphite", "Mine"),
    ("Gypsum and plaster", "Anhydrite"): ("Gypsum and plaster", "Anhydrite"),
    ("Gypsum and plaster", "Calcined"): ("Gypsum and plaster", "Calcined"),
    ("Gypsum and plaster", "Crede & ground"): ("Gypsum and plaster", "Crede & ground"),
    ("Gypsum and plaster", "Crude"): ("Gypsum and plaster", "Crude"),
    ("Gypsum and plaster", "Crude & calcined"): ("Gypsum and plaster", "Crude & calcined"),
    ("Gypsum and plaster", "Ground & calcined"): ("Gypsum and plaster", "Ground & calcined"),
    ("Gypsum and plaster", "Unknown"): ("Gypsum and plaster", "Unknown"),
    ("Helium", "Helium"): ("Helium", "Helium"),
    ("Indium, refinery", "Unknown"): ("Indium", "Refinery"),
    ("Iodine", "Unknown"): ("Iodine", "Unknown"),
    ("Iron ore", "Burnt pyrites"): None,
    ("Iron ore", "Unknown"): ("Iron ore", "Crude ore"),
    # The following is used for production of pig iron.
    ("Iron, pig", "Unknown"): ("Iron", "Pig iron"),
    ("Iron, steel and ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"): None,
    ("Iron, steel and ferro-alloys", "Ferro-Si-manganese & silico-speigeleisen"): None,
    ("Iron, steel and ferro-alloys", "Ferro-alloys"): None,
    ("Iron, steel and ferro-alloys", "Ferro-aluminium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-aluminium, Fe-Si-Al & Fe-Si-Mn-Al"): None,
    ("Iron, steel and ferro-alloys", "Ferro-calcium-silicon"): None,
    ("Iron, steel and ferro-alloys", "Ferro-chrome"): None,
    ("Iron, steel and ferro-alloys", "Ferro-chrome & ferro-silico-chrome"): None,
    ("Iron, steel and ferro-alloys", "Ferro-manganese"): None,
    ("Iron, steel and ferro-alloys", "Ferro-manganese & Fe-Si-Mn"): None,
    ("Iron, steel and ferro-alloys", "Ferro-manganese & spiegeleisen"): None,
    ("Iron, steel and ferro-alloys", "Ferro-molybdenum"): None,
    ("Iron, steel and ferro-alloys", "Ferro-nickel"): None,
    ("Iron, steel and ferro-alloys", "Ferro-niobium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-niobium & ferro-niobium-tantalum"): None,
    ("Iron, steel and ferro-alloys", "Ferro-phosphorus"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-aluminium, Fe-Si-Mn-Al, Fe-Si-Al-Ca"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-chrome"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-magnesium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-manganese"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-manganese-aluminium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silico-zirconium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-silicon"): None,
    ("Iron, steel and ferro-alloys", "Ferro-titanium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-titanium & Fe-Si-Ti"): None,
    ("Iron, steel and ferro-alloys", "Ferro-tungsten"): None,
    ("Iron, steel and ferro-alloys", "Ferro-tungsten & Fe-Si-W"): None,
    ("Iron, steel and ferro-alloys", "Ferro-vanadium"): None,
    ("Iron, steel and ferro-alloys", "Ferro-zirconium"): None,
    ("Iron, steel and ferro-alloys", "Ingots, blooms, billets"): None,
    ("Iron, steel and ferro-alloys", "Other ferro-alloys"): None,
    # The following is used for imports and exports of pig iron.
    ("Iron, steel and ferro-alloys", "Pig iron"): ("Iron", "Pig iron"),
    ("Iron, steel and ferro-alloys", "Pig iron & ferro-alloys"): None,
    ("Iron, steel and ferro-alloys", "Pig iron & ingots"): None,
    ("Iron, steel and ferro-alloys", "Pig iron & spiegeleisen"): None,
    ("Iron, steel and ferro-alloys", "Pig iron & sponge"): None,
    ("Iron, steel and ferro-alloys", "Powder"): None,
    ("Iron, steel and ferro-alloys", "Scrap"): None,
    ("Iron, steel and ferro-alloys", "Shot, powder, sponge, etc."): None,
    ("Iron, steel and ferro-alloys", "Silicon metal"): None,
    ("Iron, steel and ferro-alloys", "Spiegeleisen"): None,
    ("Iron, steel and ferro-alloys", "Sponge"): None,
    ("Iron, steel and ferro-alloys", "Sponge & powder"): None,
    ("Iron, steel and ferro-alloys", "Tin-plate scrap"): None,
    ("Kaolin", "Unknown"): ("Kaolin", "Unknown"),
    ("Lead", "Ores & concentrates"): None,
    ("Lead", "Refined"): ("Lead", "Refinery"),
    ("Lead", "Scrap"): None,
    ("Lead", "Unwrought"): None,
    ("Lead", "Unwrought & scrap"): None,
    ("Lead", "Unwrought & semi-manufactures"): None,
    ("Lead", "Unwrought alloys"): None,
    ("Lead, mine", "Unknown"): ("Lead", "Mine"),
    ("Lead, refined", "Unknown"): ("Lead", "Refinery"),
    ("Lithium", "Carbonate"): None,
    ("Lithium", "Lithium minerals"): ("Lithium", "Mine"),
    ("Lithium", "Lithium minerals, compounds & metal"): None,
    ("Lithium", "Metal"): ("Lithium", "Refinery"),
    ("Lithium", "Oxides"): None,
    ("Lithium minerals", "Amblygonite"): None,
    ("Lithium minerals", "Carbonate"): None,
    ("Lithium minerals", "Chloride"): None,
    ("Lithium minerals", "Lepidolite"): None,
    ("Lithium minerals", "Lithium minerals (Carbonate -Li content)"): None,
    ("Lithium minerals", "Lithium minerals (Chloride -Li content)"): None,
    ("Lithium minerals", "Lithium minerals (Li content)"): None,
    ("Lithium minerals", "Lithium minerals (hydroxide)"): None,
    ("Lithium minerals", "Petalite"): None,
    ("Lithium minerals", "Spodumene"): None,
    ("Lithium minerals", "Unknown"): None,
    ("Magnesite", "Unknown"): ("Magnesite and magnesia", "Magnesite"),
    ("Magnesite and magnesia", "Magnesia"): ("Magnesite and magnesia", "Magnesia"),
    ("Magnesite and magnesia", "Magnesite"): ("Magnesite and magnesia", "Magnesite"),
    ("Magnesite and magnesia", "Magnesite, calcined"): ("Magnesite and magnesia", "Magnesite, calcined"),
    ("Magnesite and magnesia", "Magnesite, crude"): ("Magnesite and magnesia", "Magnesite, crude"),
    ("Magnesite and magnesia", "Magnesite, crude & calcined"): (
        "Magnesite and magnesia",
        "Magnesite, crude & calcined",
    ),
    ("Magnesite and magnesia", "Unknown"): ("Magnesite and magnesia", "Unknown"),
    ("Magnesium metal, primary", "Unknown"): ("Magnesium metal", "Primary"),
    ("Manganese", "Metal"): ("Manganese", "Refinery"),
    ("Manganese", "Ores & Concentrates"): ("Manganese", "Ores & Concentrates"),
    ("Manganese ore", "Chemical"): None,
    ("Manganese ore", "Manganese ore (ferruginous)"): None,
    ("Manganese ore", "Metallurgical"): None,
    ("Manganese ore", "Unknown"): ("Manganese", "Ores & Concentrates"),
    ("Mercury", "Unknown"): ("Mercury", "Mine"),
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
    ("Mica", "Unknown"): ("Mica", "Unknown"),
    ("Mica", "Unmanufactured"): ("Mica", "Unmanufactured"),
    ("Mica", "Waste"): ("Mica", "Waste"),
    ("Molybdenum", "Metal"): ("Molybdenum", "Refinery"),
    ("Molybdenum", "Ores & concentrates"): ("Molybdenum", "Ores & concentrates"),
    ("Molybdenum", "Oxides"): None,
    ("Molybdenum", "Scrap"): None,
    ("Molybdenum, mine", "Unknown"): ("Molybdenum", "Mine"),
    # NOTE: I removed natural gas, as the units are not clear: sometimes "million cubic meters" appears, sometimes no
    #  units are explicitly mentioned, and sometimes the notes mention oil equivalent.
    ("Natural gas", "Unknown"): None,
    ("Nepheline syenite", "Nepheline concentrates"): ("Nepheline syenite", "Nepheline concentrates"),
    ("Nepheline syenite", "Nepheline-syenite"): ("Nepheline syenite", "Nepheline-syenite"),
    ("Nepheline syenite", "Unknown"): ("Nepheline syenite", "Unknown"),
    ("Nickel", "Mattes, sinters etc"): None,
    ("Nickel", "Ores & concentrates"): None,
    ("Nickel", "Ores, concentrates & scrap"): None,
    ("Nickel", "Ores, concentrates, mattes etc"): None,
    ("Nickel", "Oxide, sinter & sulfide"): None,
    ("Nickel", "Oxides"): None,
    ("Nickel", "Scrap"): None,
    ("Nickel", "Slurry, mattes, sinters etc"): None,
    ("Nickel", "Sulfide"): None,
    ("Nickel", "Unknown"): None,
    ("Nickel", "Unwrought"): None,
    ("Nickel", "Unwrought alloys"): None,
    ("Nickel", "Unwrought, mattes, sinters etc"): None,
    ("Nickel, mine", "Unknown"): ("Nickel", "Mine"),
    ("Nickel, smelter/refinery", "Sulfate"): None,
    ("Nickel, smelter/refinery", "Unknown"): ("Nickel", "Smelter or refinery"),
    ("Perlite", "Unknown"): ("Perlite", "Unknown"),
    ("Petroleum, crude", "Unknown"): ("Petroleum", "Crude"),
    ("Phosphate rock", "Aluminium phosphate"): ("Phosphate rock", "Aluminum phosphate"),
    ("Phosphate rock", "Apatite"): ("Phosphate rock", "Apatite"),
    ("Phosphate rock", "Calcium phosphates"): ("Phosphate rock", "Calcium phosphates"),
    ("Phosphate rock", "Guano"): ("Phosphate rock", "Guano"),
    ("Phosphate rock", "Unknown"): ("Phosphate rock", "Unknown"),
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
    ("Platinum group metals, mine", "Iridium"): ("Platinum group metals", "Mine, iridium"),
    ("Platinum group metals, mine", "Osmiridium"): ("Platinum group metals", "Mine, osmiridium"),
    ("Platinum group metals, mine", "Osmium"): ("Platinum group metals", "Mine, osmium"),
    ("Platinum group metals, mine", "Other platinum metals"): ("Platinum group metals", "Mine, other"),
    ("Platinum group metals, mine", "Palladium"): ("Platinum group metals", "Mine, palladium"),
    ("Platinum group metals, mine", "Platinum"): ("Platinum group metals", "Mine, platinum"),
    ("Platinum group metals, mine", "Rhodium"): ("Platinum group metals", "Mine, rhodium"),
    ("Platinum group metals, mine", "Ruthenium"): ("Platinum group metals", "Mine, ruthenium"),
    ("Platinum group metals, mine", "Unknown"): ("Platinum group metals", "Mine, unknown"),
    ("Potash", "Carbonate"): ("Potash", "Carbonate"),
    ("Potash", "Caustic potash"): ("Potash", "Caustic potash"),
    ("Potash", "Chlorate"): ("Potash", "Chlorate"),
    ("Potash", "Chloride"): ("Potash", "Chloride"),
    ("Potash", "Cyanide"): ("Potash", "Cyanide"),
    ("Potash", "Fertiliser salts"): ("Potash", "Fertilizer salts"),
    ("Potash", "Kainite, sylvinite"): ("Potash", "Kainite, sylvinite"),
    ("Potash", "Nitrate"): ("Potash", "Nitrate"),
    ("Potash", "Other fertiliser salts"): ("Potash", "Other fertilizer salts"),
    ("Potash", "Other potassic chemicals"): ("Potash", "Other potassic chemicals"),
    ("Potash", "Other potassic fertilisers"): ("Potash", "Other potassic fertilizers"),
    ("Potash", "Polyhalite"): ("Potash", "Polyhalite"),
    ("Potash", "Potassic chemicals"): ("Potash", "Potassic chemicals"),
    ("Potash", "Potassic fertilisers"): ("Potash", "Potassic fertilizers"),
    ("Potash", "Potassic salts"): ("Potash", "Potassic salts"),
    ("Potash", "Sulfide"): ("Potash", "Sulfide"),
    ("Potash", "Unknown"): ("Potash", "Unknown"),
    ("Rare earth minerals", "Bastnaesite"): None,
    ("Rare earth minerals", "Loparite"): None,
    ("Rare earth minerals", "Monazite"): None,
    ("Rare earth minerals", "Unknown"): ("Rare earths", "Ores & concentrates"),
    ("Rare earth minerals", "Xenotime"): None,
    ("Rare earth oxides", "Unknown"): None,
    ("Rare earths", "Cerium compounds"): None,
    ("Rare earths", "Cerium metal"): None,
    ("Rare earths", "Ferro-cerium & other pyrophoric alloys"): None,
    ("Rare earths", "Metals"): ("Rare earths", "Refinery"),
    ("Rare earths", "Ores & concentrates"): ("Rare earths", "Ores & concentrates"),
    ("Rare earths", "Other rare earth compounds"): None,
    ("Rare earths", "Rare earth compounds"): ("Rare earths", "Compounds"),
    ("Rhenium", "Unknown"): ("Rhenium", "Unknown"),
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
    ("Salt", "Unknown"): ("Salt", "Unknown"),
    ("Selenium, refined", "Unknown"): ("Selenium", "Refinery"),
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
    ("Sillimanite minerals", "Unknown"): ("Sillimanite minerals", "Unknown"),
    ("Silver", "Alloys"): None,
    ("Silver", "Metal"): ("Silver", "Metal"),
    ("Silver", "Metal, refined"): None,
    ("Silver", "Metal, unrefined"): None,
    ("Silver", "Ores & concentrates"): ("Silver", "Ores & concentrates"),
    ("Silver", "Silver-lead bullion"): None,
    ("Silver", "Unknown"): None,
    ("Silver", "Waste & scrap"): None,
    ("Silver, mine", "Unknown"): ("Silver", "Mine"),
    ("Sodium carbonate, natural", "Unknown"): ("Sodium carbonate", "Natural"),
    ("Steel, crude", "Unknown"): ("Steel", "Crude"),
    ("Strontium minerals", "Unknown"): ("Strontium minerals", "Unknown"),
    ("Sulphur and pyrites", "Other"): ("Sulphur and pyrites", "Other"),
    ("Sulphur and pyrites", "Precipitated"): ("Sulphur and pyrites", "Precipitated"),
    ("Sulphur and pyrites", "Pyrites"): ("Sulphur and pyrites", "Pyrites"),
    ("Sulphur and pyrites", "Pyrites - cupreous"): ("Sulphur and pyrites", "Pyrites - cupreous"),
    ("Sulphur and pyrites", "Pyrites - iron"): None,
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
    ("Talc", "Unknown"): ("Talc", "Unknown"),
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
    ("Tantalum and niobium", "Tin slags, Nb content"): ("Tantalum and niobium", "Tin slags, niobium content"),
    ("Tantalum and niobium", "Tin slags, Ta content"): ("Tantalum and niobium", "Tin slags, tantalum content"),
    ("Tantalum and niobium", "Unknown"): ("Tantalum and niobium", "Unknown"),
    ("Tantalum and niobium minerals", "Columbite"): ("Tantalum and niobium minerals", "Columbite"),
    ("Tantalum and niobium minerals", "Columbite- Nb content"): (
        "Tantalum and niobium minerals",
        "Columbite, niobium content",
    ),
    ("Tantalum and niobium minerals", "Columbite- Ta content"): (
        "Tantalum and niobium minerals",
        "Columbite, tantalum content",
    ),
    ("Tantalum and niobium minerals", "Columbite-tantalite"): ("Tantalum and niobium minerals", "Columbite-tantalite"),
    ("Tantalum and niobium minerals", "Columbite-tantalite-Nb content"): (
        "Tantalum and niobium minerals",
        "Columbite-tantalite-niobium content",
    ),
    ("Tantalum and niobium minerals", "Columbite-tantalite-Ta content"): (
        "Tantalum and niobium minerals",
        "Columbite-tantalite-tantalum content",
    ),
    ("Tantalum and niobium minerals", "Djalmaite"): ("Tantalum and niobium minerals", "Djalmaite"),
    ("Tantalum and niobium minerals", "Microlite"): ("Tantalum and niobium minerals", "Microlite"),
    ("Tantalum and niobium minerals", "Pyrochlore"): ("Tantalum and niobium minerals", "Pyrochlore"),
    ("Tantalum and niobium minerals", "Pyrochlore -Nb content"): (
        "Tantalum and niobium minerals",
        "Pyrochlore, niobium content",
    ),
    ("Tantalum and niobium minerals", "Struverite"): ("Tantalum and niobium minerals", "Struverite"),
    ("Tantalum and niobium minerals", "Struverite (Ta content)"): (
        "Tantalum and niobium minerals",
        "Struverite, tantalum content",
    ),
    ("Tantalum and niobium minerals", "Tantalite"): ("Tantalum and niobium minerals", "Tantalite"),
    ("Tantalum and niobium minerals", "Tantalite -Ta content"): (
        "Tantalum and niobium minerals",
        "Tantalite, tantalum content",
    ),
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Nb content)"): (
        "Tantalum and niobium minerals",
        "Tantalum & Niobium, niobium content",
    ),
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Ta content)"): (
        "Tantalum and niobium minerals",
        "Tantalum & Niobium, tantalum content",
    ),
    ("Tellurium, refined", "Unknown"): ("Tellurium", "Refinery"),
    ("Tin", "Concentrates"): None,
    ("Tin", "Scrap"): None,
    ("Tin", "Tin-silver ore"): None,
    ("Tin", "Unwrought"): None,
    ("Tin", "Unwrought & scrap"): None,
    ("Tin", "Unwrought & semi-manufactures"): None,
    ("Tin", "Unwrought alloys"): None,
    ("Tin, mine", "Unknown"): ("Tin", "Mine"),
    ("Tin, smelter", "Unknown"): ("Tin", "Smelter"),
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
    ("Titanium minerals", "Unknown"): ("Titanium minerals", "Unknown"),
    ("Tungsten", "Ammonium paratungstate"): ("Tungsten", "Ammonium paratungstate"),
    ("Tungsten", "Carbide"): ("Tungsten", "Carbide"),
    ("Tungsten", "Metal"): ("Tungsten", "Metal"),
    ("Tungsten", "Ores & concentrates"): ("Tungsten", "Ores & concentrates"),
    ("Tungsten", "Other tungsten ores"): ("Tungsten", "Other tungsten ores"),
    ("Tungsten", "Powder"): ("Tungsten", "Powder"),
    ("Tungsten", "Scheelite ores & concentrates"): ("Tungsten", "Scheelite ores & concentrates"),
    ("Tungsten", "Unknown"): ("Tungsten", "Unknown"),
    ("Tungsten", "Wolframite ores & concentrates"): ("Tungsten", "Wolframite ores & concentrates"),
    ("Tungsten, mine", "Unknown"): ("Tungsten", "Mine"),
    ("Uranium", "Unknown"): ("Uranium", "Unknown"),
    ("Vanadium", "Lead vanadium concentrates"): ("Vanadium", "Lead vanadium concentrates"),
    ("Vanadium", "Metal"): ("Vanadium", "Metal"),
    ("Vanadium", "Ores & concentrates"): ("Vanadium", "Ores & concentrates"),
    ("Vanadium", "Pentoxide"): ("Vanadium", "Pentoxide"),
    ("Vanadium", "Vanadiferous residues"): ("Vanadium", "Vanadiferous residues"),
    ("Vanadium", "Vanadium-titanium pig iron"): None,
    ("Vanadium, mine", "Unknown"): ("Vanadium", "Mine"),
    ("Vermiculite", "Unknown"): ("Vermiculite", "Unknown"),
    ("Wollastonite", "Unknown"): ("Wollastonite", "Unknown"),
    ("Zinc", "Crude & refined"): None,
    ("Zinc", "Ores & concentrates"): None,
    ("Zinc", "Oxides"): None,
    ("Zinc", "Scrap"): None,
    ("Zinc", "Unwrought"): None,
    ("Zinc", "Unwrought alloys"): None,
    ("Zinc, mine", "Unknown"): ("Zinc", "Mine"),
    ("Zinc, slab", "Unknown"): ("Zinc", "Refinery"),
    ("Zirconium", "Concentrates"): ("Zirconium", "Concentrates"),
    ("Zirconium", "Metal"): ("Zirconium", "Metal"),
    ("Zirconium", "Unknown"): ("Zirconium", "Unknown"),
    ("Zirconium", "Zirconium sand"): ("Zirconium", "Zirconium sand"),
    ("Zirconium minerals", "Unknown"): ("Zirconium minerals", "Unknown"),
}

# Mapping from original unit names to tonnes.
# NOTE: The keys in this dictionary should coincide with all units found in the data.
UNIT_MAPPING = {
    "tonnes": "tonnes",
    "tonnes (metric)": "tonnes",
    "tonnes (Al2O3 content)": "tonnes of aluminum oxide content",
    "tonnes (K20 content)": "tonnes of potassium oxide content",
    "tonnes (metal content)": "tonnes of metal content",
    # NOTE: The following units will be converted to tonnes using conversion factors.
    "kilograms": "tonnes",
    "kilograms (metal content)": "tonnes of metal content",
    "Carats": "tonnes",
    "million cubic metres": "tonnes",
}

# Some of those "tonnes *" can safely be mapped to simply "tonnes".
# Given that this data is later combined wigh USGS data (given in tonnes), we need to ensure that they mean the
# same thing.
# So, to be conservative, go to the explorer and inspect those minerals that come as "tonnes *"; compare them to the USGS current data (given in "tonnes"); if they are in reasonable agreement, add them to the following list.
# Their unit will be converted to "tonnes", and hence combined with USGS data.
MINERALS_TO_CONVERT_TO_TONNES = [
    "Cement",
    "Cobalt",
    "Copper",
    "Lead",
    "Molybdenum",
    "Nickel",
    "Silver",
    "Tin",
    "Zinc",
]

# Footnotes (that will appear in the footer of charts) to add to the flattened output table.
FOOTNOTES = {
    # Example:
    # 'production|Tungsten|Powder|tonnes': "Tungsten includes...",
}

# There are many historical regions with overlapping data with their successor countries.
# Accept only overlaps on the year when the historical country stopped existing.
ACCEPTED_OVERLAPS = [
    {1991: {"USSR", "Armenia"}},
    # {1991: {"USSR", "Belarus"}},
    {1991: {"USSR", "Russia"}},
    {1992: {"Czechia", "Czechoslovakia"}},
    {1992: {"Slovakia", "Czechoslovakia"}},
    {1990: {"Germany", "East Germany"}},
    {1990: {"Germany", "West Germany"}},
    {2010: {"Netherlands Antilles", "Bonaire Sint Eustatius and Saba"}},
    {1990: {"Yemen", "Yemen People's Republic"}},
]


def harmonize_commodity_subcommodity_pairs(tb: Table) -> Table:
    tb = tb.astype({"commodity": str, "sub_commodity": str}).copy()
    missing_mappings = set(
        [tuple(pair) for pair in tb[["commodity", "sub_commodity"]].drop_duplicates().values.tolist()]
    ) - set(COMMODITY_MAPPING)
    assert len(missing_mappings) == 0, f"Missing mappings: {missing_mappings}"
    # NOTE: Do not assert that all mappings are used, since mappings are shared for imports, exports and production.
    # unused_mappings = set(COMMODITY_MAPPING) - set([tuple(pair) for pair in df[["commodity", "sub_commodity"]].drop_duplicates().values.tolist()])
    # assert len(unused_mappings) == 0, f"Unused mappings: {unused_mappings}"
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

    # In some cases, units given in "tonnes *" can safely be converted to simply "tonnes".
    # See explanation above, where MINERALS_TO_CONVERT_TO_TONNES is defined.
    # But note that some of these minerals have units that need to be converted (since they are not in tonnes).
    # Therefore, make a list of all those minerals that need to be converted later.
    minerals_to_convert_to_tonnes_later = (
        tb[tb["commodity"].isin(MINERALS_TO_CONVERT_TO_TONNES) & (~tb["unit"].str.startswith("tonnes"))]["commodity"]
        .unique()
        .tolist()
    )
    tb.loc[
        tb["commodity"].isin(set(MINERALS_TO_CONVERT_TO_TONNES) - set(minerals_to_convert_to_tonnes_later)), "unit"
    ] = "tonnes"

    # Check that, for each category-commodity-subcommodity, there is only one unit (or none).
    group = tb.groupby(["category", "commodity", "sub_commodity"], observed=True, as_index=False)
    unit_count = group.agg({"unit": "nunique"})
    assert unit_count[
        unit_count["unit"] > 1
    ].empty, "Multiple units found for the same category-commodity-subcommodity."
    # Given that the unit is sometimes given and sometimes not (quite arbitrarily, as mentioned in the meadow step),
    # first attempt to fill empty units from the same category-commodity-subcommodity combination.
    tb["unit"] = group["unit"].transform(lambda x: x.ffill().bfill())

    # Visually inspect category-commodity-subcommodity combinations with missing units.
    # tb[tb["unit"].isnull()][["category", "commodity", "sub_commodity"]].drop_duplicates()

    # After this, still 82 combinations have no unit (only in Imports and Exports).
    # In these cases, attempt to fill unit based on category-commodity.
    # Check that, for each category-commodity, there is only one unit (or none).
    group = tb.groupby(["category", "commodity"], observed=True, as_index=False)
    unit_count = group.agg({"unit": "nunique"})
    assert unit_count[unit_count["unit"] > 1].empty, "Multiple units found for the same category-commodity."
    # Fill empty units from the same category-commodity combination.
    tb["unit"] = group["unit"].transform(lambda x: x.ffill().bfill())

    # NOTE: We decided for now to not include "Sponge". If we change our mind, we may need to uncomment the following.
    # # Since we are mapping ("Iron, steel and ferro-alloys", "Sponge"): ("Iron", "Sponge"), this leads to imports having
    # # no units. This may be fixed after further harmonization. If so, remove this part of the code.
    # # If this is not fixed, then consider a different solution.
    # error = "Something has changed in the units of Iron Sponge. Check this part of the code."
    # assert set(
    #     tb.loc[(tb["category"] == "Imports") & (tb["commodity"] == "Iron") & (tb["sub_commodity"] == "Sponge")]["unit"]
    # ) == set([pd.NA]), error
    # tb.loc[(tb["commodity"] == "Iron") & (tb["sub_commodity"] == "Sponge"), "unit"] = "tonnes"

    # Visually inspect category-commodity-subcommodity combinations with missing units.
    # Check that the only combinations still with no units are the expected ones.
    missing_units = {
        "category": ["Exports", "Imports"],
        "commodity": ["Gemstones", "Gemstones"],
        "sub_commodity": ["Unknown", "Unknown"],
    }
    error = "The list of combinations category-commodity-subcommodity with missing units has changed."
    assert (
        tb[tb["unit"].isnull()][["category", "commodity", "sub_commodity"]].drop_duplicates().to_dict(orient="list")
        == missing_units
    ), error

    # I tried to figure out the missing units, but I found contradictory information:
    # https://www2.bgs.ac.uk/mineralsUK/statistics/wms.cfc?method=listResults&dataType=Exports&commodity=783&dateFrom=2015&dateTo=2022&country=&agreeToTsAndCs=agreed
    # See that, for example, Turkey exports of gemstones in 2016 appears twice, with 12545 and 1315.
    # A similar issue happens for many other countries.
    # These results make no sense, so I'll simply drop these rows.
    tb = tb[tb["unit"].notnull()].reset_index(drop=True)

    units = sorted(set(tb["unit"]))

    # The unit of diamonds (Carats) is sometimes explicitly given on top of the table, but not always
    # (see e.g. exports between 2010 and 2019).
    # Additionally, in the table footnotes they explain that sometimes the value is in Pounds!
    # Therefore, assume that the unit is always carats (and convert to tonnes), and, where that footnote appears, simply
    # remove the value.
    tb.loc[(tb["commodity"] == "Diamond"), "unit"] = "Carats"
    tb.loc[(tb["commodity"] == "Diamond") & (tb["note"].str.lower().str.contains("pounds")), "value"] = None

    # Sanity check.
    error = "Unexpected units found. Add them to the unit mapping and decide its conversion."
    assert set(units) == set(UNIT_MAPPING), error

    for unit in units:
        mask = tb["unit"] == unit
        if unit in [
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
            # Assert that commodity is either helium or natural gas, and convert accordingly.
            # NOTE: I decided to remove natural gas (see notes above in the commodity mapping).
            error = "Unexpected commodity using million cubic metres."
            assert set(tb[mask]["commodity"]) == {"Helium"}, error
            tb.loc[mask & (tb["commodity"] == "Helium"), "value"] *= MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES
            # tb.loc[mask & (tb["commodity"] == "Natural gas"), "value"] *= MILLION_CUBIC_METERS_OF_NATURAL_GAS_TO_TONNES
        tb.loc[mask, "unit"] = UNIT_MAPPING[unit]

    # Handle the rest of minerals that need to be converted to "tonnes" (after applying a conversion factor).
    # See explanation above in this function.
    tb.loc[tb["commodity"].isin(minerals_to_convert_to_tonnes_later), "unit"] = "tonnes"

    return tb


def remove_data_from_non_existing_regions(tb: Table) -> Table:
    # Remove data for historical regions after they stop existing, and successor countries before they came to existence.
    tb.loc[(tb["country"] == "USSR") & (tb["year"] > 1991), "value"] = None
    tb.loc[(tb["country"] == "Armenia") & (tb["year"] == 1990), "value"] = None
    tb.loc[(tb["country"] == "North Macedonia") & (tb["year"] == 1991), "value"] = None
    tb.loc[(tb["country"] == "Czechia") & (tb["year"] == 1990), "value"] = None
    tb.loc[(tb["country"] == "East Germany") & (tb["year"] > 1990), "value"] = None
    tb.loc[(tb["country"].isin(["Serbia", "Montenegro"])) & (tb["year"].isin([2002, 2003, 2004, 2005])), "value"] = None
    tb.loc[
        (tb["country"].isin(["Aruba", "Bonaire Sint Eustatius and Saba"]))
        & (tb["year"].isin([2001, 2002, 2007, 2008, 2009])),
        "value",
    ] = None
    tb.loc[(tb["country"] == "Yemen") & (tb["year"].isin([1987, 1988, 1989, 1991])), "value"] = None
    tb = tb.dropna(subset="value").reset_index(drop=True)

    return tb


def clean_notes(notes):
    notes_clean = []
    # After creating region aggregates, some notes become nan.
    # But in all other cases, notes are lists (either empty or filled with strings).
    # Therefore, pd.isnull(notes) returns either a boolean or a numpy array.
    # If it's a boolean, it means that all notes are nan (but just to be sure, also check that the boolean is True).
    is_null = pd.isnull(notes)
    if isinstance(is_null, bool) and is_null:
        return notes_clean

    for note in notes:
        if len(note) > 1:
            if "ms excel" in note.lower():
                # Skip unnecessary notes about how to download the data.
                continue
            # Ensure each note starts with a capital letter, and ends in a single period.
            # NOTE: Using capitalize() would make all characters lower case except the first.
            note = note[0].upper() + (note[1:].replace("\xa0", " ") + ".").replace("..", ".")
            if note not in notes_clean:
                notes_clean.append(note)

    return notes_clean


def gather_notes(tb: Table, notes_columns: List[str]) -> Dict[str, str]:
    # Create another table with the same structure, but containing notes.
    tb_flat_notes = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=notes_columns,
        join_column_levels_with="|",
    )
    tb_flat_notes = tb_flat_notes.rename(
        columns={column: column.replace("notes_", "") for column in tb_flat_notes.columns}
    )

    # Gather all notes for each column.
    notes_dict = {}
    for column in tqdm(tb_flat_notes.drop(columns=["country", "year"]).columns):
        _notes = tb_flat_notes[column].dropna().tolist()
        if len(_notes) > 0:
            # Gather all notes for this column.
            notes = sum(_notes, [])
            # Get unique notes keeping the order.
            notes = pd.unique(pd.Series(notes)).tolist()
            # Join notes.
            if len(notes) > 0:
                notes_str = "- " + "\n- ".join(notes)
                notes_dict[column] = notes_str

    return notes_dict


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

    # Remove data for regions that did not exist at the time.
    tb = remove_data_from_non_existing_regions(tb=tb)

    # Improve the name of the commodities.
    tb["commodity"] = tb["commodity"].str.capitalize()

    # Harmonize commodity-subcommodity pairs.
    tb = harmonize_commodity_subcommodity_pairs(tb=tb)

    # Harmonize units.
    tb = harmonize_units(tb=tb)

    # Pivot table to have a column for each category.
    tb = (
        tb.pivot(
            index=["country", "year", "commodity", "sub_commodity", "unit"],
            columns="category",
            values=["value", "note", "general_notes"],
            join_column_levels_with="_",
        )
        .underscore()
        .rename(
            columns={"value_production": "production", "value_imports": "imports", "value_exports": "exports"},
            errors="raise",
        )
    )

    # Set an appropriate format for value columns.
    tb = tb.astype({column: "Float64" for column in ["production", "imports", "exports"]})

    # Parse notes as lists of strings.
    for column in [
        "note_production",
        "note_imports",
        "note_exports",
        "general_notes_production",
        "general_notes_imports",
        "general_notes_exports",
    ]:
        tb[column] = tb[column].fillna("[]").apply(ast.literal_eval)

    # Add regions to the table.
    REGIONS = {**geo.REGIONS, **{"World": {}}}
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        index_columns=["country", "year", "commodity", "sub_commodity", "unit"],
        accepted_overlaps=ACCEPTED_OVERLAPS,
    )

    # Clean notes columns, and combine notes at the individual row level with general table notes.
    for category in ["exports", "imports", "production"]:
        tb[f"notes_{category}"] = [
            clean_notes(note) for note in tb[f"note_{category}"] + tb[f"general_notes_{category}"]
        ]
        # Drop unnecessary columns.
        tb = tb.drop(columns=[f"note_{category}", f"general_notes_{category}"])

    # Gather all notes in a dictionary.
    notes = gather_notes(tb, notes_columns=["notes_exports", "notes_imports", "notes_production"])

    # Create a wide table.
    tb_flat = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["exports", "imports", "production"],
        join_column_levels_with="|",
    )

    # NOTE: Here, I could loop over columns and improve metadata.
    # However, for convenience (since this step is not used separately), this will be done in the garden minerals step.
    # So, for now, simply add titles and descriptions from producer.
    for column in tb_flat.drop(columns=["country", "year"]).columns:
        # Create metadata title (before they become snake-case).
        tb_flat[column].metadata.title = column
        if column in notes:
            tb_flat[column].metadata.description_from_producer = "Notes found in original BGS data:\n" + notes[column]

    # To avoid ETL failing when storing the table, convert lists of notes to strings (and add metadata).
    for column in ["notes_imports", "notes_exports", "notes_production"]:
        tb[column] = tb[column].copy_metadata(tb["production"]).astype(str)

    # Add footnotes.
    for column, note in FOOTNOTES.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    # Drop empty columns.
    tb_flat = tb_flat.dropna(axis=1, how="all").reset_index(drop=True)

    # Format table conveniently.
    # NOTE: All commodities have the same unit for imports, exports and production except one:
    #  Potash Chloride uses "tonnes" for imports and exports, and "tonnes of K20 content" (which is also misspelled).
    #  Due to this, the index cannot simply be "country", "year", "commodity", "sub_commodity"; we need also "unit".
    # counts = tb.groupby(["commodity", "sub_commodity", "country", "year"], observed=True, as_index=False).nunique()
    # counts[counts["unit"] > 1][["commodity", "sub_commodity"]].drop_duplicates()
    tb = tb.format(["country", "year", "commodity", "sub_commodity", "unit"])
    tb_flat = tb_flat.format(["country", "year"], short_name=paths.short_name + "_flat")
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns})

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_flat], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
