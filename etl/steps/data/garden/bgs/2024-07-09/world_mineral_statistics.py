"""Load a meadow dataset and create a garden dataset."""

import ast
from typing import Dict, List

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table, VariablePresentationMeta
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.files import ruamel_load
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
    ("Alumina", "Unknown"): ("Alumina", "Refinery"),
    ("Aluminium, primary", "Unknown"): ("Aluminum", "Smelter"),
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
    ("Arsenic", "Metallic arsenic"): None,
    ("Arsenic", "Unknown"): None,
    ("Arsenic", "White arsenic"): ("Arsenic", "Processing"),
    ("Arsenic, white", "Unknown"): ("Arsenic", "Processing"),
    ("Asbestos", "Amosite"): None,
    ("Asbestos", "Amphibole"): None,
    ("Asbestos", "Anthophyllite"): None,
    ("Asbestos", "Chrysotile"): None,
    ("Asbestos", "Crocidolite"): None,
    ("Asbestos", "Unknown"): ("Asbestos", "Mine"),
    ("Asbestos, unmanufactured", "Amosite"): None,
    ("Asbestos, unmanufactured", "Chrysotile"): None,
    ("Asbestos, unmanufactured", "Crocidolite"): None,
    # NOTE: In the original BGS data, there is "Asbestos, unmanufactured" with subcommodity "Other manufactured".
    #  It's unclear what this means, but I'll assume that it's a mistake and that it should be other unmanufactured.
    #  This happens, e.g. to USA 1986 imports.
    ("Asbestos, unmanufactured", "Other manufactured"): None,
    ("Asbestos, unmanufactured", "Unmanufactured"): ("Asbestos", "Mine"),
    ("Asbestos, unmanufactured", "Unmanufactured, crude"): None,
    ("Asbestos, unmanufactured", "Unmanufactured, fibre"): None,
    ("Asbestos, unmanufactured", "Unmanufactured, shorts"): None,
    ("Asbestos, unmanufactured", "Unmanufactured, waste"): None,
    ("Asbestos, unmanufactured", "Waste"): None,
    ("Barytes", "Barium minerals"): ("Barite", "Barium minerals"),
    # NOTE: Only imports/exports data prior to 2009.
    ("Barytes", "Barytes"): None,
    ("Barytes", "Unknown"): ("Barite", "Mine"),
    ("Barytes", "Witherite"): ("Barite", "Witherite"),
    # NOTE: The following is used for production of bauxite.
    ("Bauxite", "Unknown"): ("Bauxite", "Mine"),
    ("Bauxite, alumina and aluminium", "Alumina"): ("Alumina", "Refinery"),
    ("Bauxite, alumina and aluminium", "Alumina hydrate"): None,
    # NOTE: The following is used for imports and exports of bauxite.
    ("Bauxite, alumina and aluminium", "Bauxite"): ("Bauxite", "Mine"),
    ("Bauxite, alumina and aluminium", "Bauxite, calcined"): None,
    ("Bauxite, alumina and aluminium", "Bauxite, crude dried"): None,
    ("Bauxite, alumina and aluminium", "Bauxite, dried"): None,
    ("Bauxite, alumina and aluminium", "Bauxite, uncalcined"): None,
    ("Bauxite, alumina and aluminium", "Scrap"): None,
    ("Bauxite, alumina and aluminium", "Unwrought"): None,
    ("Bauxite, alumina and aluminium", "Unwrought & scrap"): None,
    ("Bauxite, alumina and aluminium", "Unwrought alloys"): None,
    # NOTE: The following could be mapped to ("Clays", "Mine, attapulgite"). We decided to remove "Clays".
    ("Bentonite and fuller's earth", "Attapulgite"): None,
    # NOTE: The following could be mapped to ("Clays", "Mine, bentonite"). We decided to remove "Clays".
    ("Bentonite and fuller's earth", "Bentonite"): None,
    # NOTE: The following could be mapped to ("Clays", "Mine, fuller's earth"). We decided to remove "Clays".
    ("Bentonite and fuller's earth", "Fuller's earth"): None,
    # NOTE: The following could be mapped to ("Clays", "Mine, sepiolite"). We decided to remove "Clays".
    ("Bentonite and fuller's earth", "Sepiolite"): None,
    ("Bentonite and fuller's earth", "Unknown"): None,
    # NOTE: Beryl data may have a data issue: The biggest producer is Namibia, which goes from 15 in 1993 to 15000 in
    #  2021. Discard for now.
    ("Beryl", "Unknown"): None,
    ("Bismuth", "Compounds"): ("Bismuth", "Compounds"),
    ("Bismuth", "Metal"): ("Bismuth", "Metal"),
    ("Bismuth", "Ores & concentrates"): ("Bismuth", "Ores & concentrates"),
    ("Bismuth, mine", "Unknown"): ("Bismuth", "Mine"),
    ("Borates", "Unknown"): None,
    ("Bromine", "Compounds"): ("Bromine", "Compounds"),
    ("Bromine", "Unknown"): ("Bromine", "Processing"),
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
    # NOTE: The following has only imports/exports data.
    ("Chromium", "Metal"): None,
    # NOTE: The following has only imports/exports data.
    ("Chromium", "Ores & concentrates"): None,
    ("Chromium ores and concentrates", "Unknown"): ("Chromium", "Mine, gross weight"),
    # NOTE: All subcommodities of coal production will be summed up into one.
    ("Coal", "Anthracite"): ("Coal", "Mine, anthracite"),
    ("Coal", "Anthracite & Bituminous"): ("Coal", "Mine, anthracite & Bituminous"),
    # NOTE: Old data.
    ("Coal", "Anthracite & semi-bituminous"): None,
    # NOTE: Old data.
    ("Coal", "Anthracite (mined)"): None,
    # NOTE: Old data.
    ("Coal", "Anthracite (opencast)"): None,
    ("Coal", "Bituminous"): ("Coal", "Mine, bituminous"),
    ("Coal", "Bituminous & lignite"): ("Coal", "Mine, bituminous & lignite"),
    # NOTE: Old data.
    ("Coal", "Bituminous (mined)"): None,
    # NOTE: Old data.
    ("Coal", "Bituminous (opencast)"): None,
    ("Coal", "Briquettes"): None,
    ("Coal", "Brown coal"): ("Coal", "Mine, brown coal"),
    ("Coal", "Brown coal & lignite"): ("Coal", "Mine, brown coal & lignite"),
    # NOTE: The following sounds like it could be a total, but it's not. It is complementary to all other coal subtypes.
    # Therefore, we will combine all coal subtypes into one (and check that there is no double-counting).
    ("Coal", "Coal"): ("Coal", "Mine, other"),
    ("Coal", "Coking coal"): ("Coal", "Mine, coking coal"),
    ("Coal", "Hard coal"): ("Coal", "Mine, hard coal"),
    ("Coal", "Lignite"): ("Coal", "Mine, lignite"),
    ("Coal", "Other bituminous coal"): ("Coal", "Mine, other bituminous coal"),
    ("Coal", "Other coal"): None,
    ("Coal", "Sub-bituminous"): ("Coal", "Mine, sub-bituminous"),
    ("Coal", "Unknown"): ("Coal", "Unspecified"),
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
    ("Diamond", "Cut"): None,
    ("Diamond", "Dust"): None,
    ("Diamond", "Gem"): None,
    ("Diamond", "Gem, cut"): None,
    ("Diamond", "Gem, rough"): None,
    ("Diamond", "Industrial"): ("Diamond", "Mine, industrial"),
    ("Diamond", "Other"): None,
    ("Diamond", "Rough"): None,
    ("Diamond", "Rough & Cut"): None,
    ("Diamond", "Unknown"): None,
    ("Diamond", "Unsorted"): None,
    ("Diatomite", "Activated diatomite"): None,
    ("Diatomite", "Moler"): None,
    ("Diatomite", "Moler bricks"): None,
    ("Diatomite", "Unknown"): ("Diatomite", "Mine"),
    ("Feldspar", "Unknown"): ("Feldspar", "Mine"),
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
    ("Fluorspar", "Unknown"): ("Fluorspar", "Mine"),
    ("Gallium, primary", "Unknown"): ("Gallium", "Processing"),
    ("Gemstones", "Unknown"): None,
    ("Germanium metal", "Unknown"): ("Germanium", "Refinery"),
    ("Gold", "Metal"): ("Gold", "Metal"),
    ("Gold", "Metal, other"): None,
    ("Gold", "Metal, refined"): ("Gold", "Metal, refined"),
    ("Gold", "Metal, unrefined"): None,
    ("Gold", "Ores & concentrates"): ("Gold", "Ores & concentrates"),
    ("Gold", "Ores, concentrates & unrefined metal"): None,
    ("Gold", "Waste & scrap"): None,
    ("Gold, mine", "Unknown"): ("Gold", "Mine"),
    ("Graphite", "Unknown"): ("Graphite", "Mine"),
    ("Gypsum and plaster", "Anhydrite"): None,
    ("Gypsum and plaster", "Calcined"): ("Gypsum", "Calcined"),
    ("Gypsum and plaster", "Crede & ground"): None,
    ("Gypsum and plaster", "Crude"): ("Gypsum", "Crude"),
    ("Gypsum and plaster", "Crude & calcined"): None,
    ("Gypsum and plaster", "Ground & calcined"): None,
    ("Gypsum and plaster", "Unknown"): ("Gypsum", "Mine"),
    ("Helium", "Helium"): ("Helium", "Mine"),
    ("Indium, refinery", "Unknown"): ("Indium", "Refinery"),
    ("Iodine", "Unknown"): ("Iodine", "Mine"),
    ("Iron ore", "Burnt pyrites"): None,
    ("Iron ore", "Unknown"): ("Iron ore", "Mine, crude ore"),
    # The following is used for production of pig iron.
    ("Iron, pig", "Unknown"): ("Iron", "Smelter, pig iron"),
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
    ("Iron, steel and ferro-alloys", "Pig iron"): ("Iron", "Smelter, pig iron"),
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
    # NOTE: The following could be mapped to ("Clays", "Mine, kaolin"). We decided to remove "Clays".
    ("Kaolin", "Unknown"): None,
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
    # NOTE: It's unclear what "Unknown" is.
    ("Magnesite", "Unknown"): None,
    ("Magnesite and magnesia", "Magnesia"): ("Magnesium compounds", "Magnesia"),
    ("Magnesite and magnesia", "Magnesite"): ("Magnesium compounds", "Magnesite"),
    # NOTE: Magnesite, calcined only has data prior to 1985.
    ("Magnesite and magnesia", "Magnesite, calcined"): None,
    # NOTE: Magnesite, crude only has data prior to 1985.
    ("Magnesite and magnesia", "Magnesite, crude"): None,
    # NOTE: Magnesite, crude & calcined only has data prior to 1985.
    ("Magnesite and magnesia", "Magnesite, crude & calcined"): None,
    # NOTE: It's unclear what "Unknown" is.
    ("Magnesite and magnesia", "Unknown"): None,
    ("Magnesium metal, primary", "Unknown"): ("Magnesium metal", "Smelter"),
    ("Manganese", "Metal"): ("Manganese", "Refinery"),
    # NOTE: The following could be mapped to ("Manganese", "Mine, ores & concentrates"), but we decided to discard it.
    ("Manganese", "Ores & Concentrates"): None,
    ("Manganese ore", "Chemical"): None,
    ("Manganese ore", "Manganese ore (ferruginous)"): None,
    ("Manganese ore", "Metallurgical"): None,
    # NOTE: The following could be mapped to ("Manganese", "Mine, ores & concentrates"), but we decided to discard it.
    ("Manganese ore", "Unknown"): None,
    ("Mercury", "Unknown"): ("Mercury", "Mine"),
    # NOTE: All mica data below is very sparse. Several have data until 1980, or 2002.
    #  The sub-commodity with the largest numbers is "Unknown", so it's not clear what it means.
    #  None of the sub-commodities seem to agree with USGS data.
    ("Mica", "Block"): None,
    ("Mica", "Condenser films"): None,
    # NOTE: "Crude" only has production data, and only until 1980.
    ("Mica", "Crude"): None,
    ("Mica", "Ground"): ("Mica", "Ground"),
    ("Mica", "Ground & waste"): None,
    # NOTE: The following is ambiguous, and has only imports/exports data until 2013/2007.
    ("Mica", "Mica"): None,
    ("Mica", "Other unmanufactured"): None,
    # NOTE: "Phlogopite" only has production data, and only until 1980.
    ("Mica", "Phlogopite"): None,
    # NOTE: "Sheet" only has production data, and only until 1980.
    ("Mica", "Sheet"): None,
    # NOTE: "Splittings" has only imports/exports data until 2002.
    ("Mica", "Splittings"): None,
    # NOTE: The following is the sub-commodity with the largest numbers, even though it's not specified what it shows.
    ("Mica", "Unknown"): ("Mica", "Mine"),
    ("Mica", "Unmanufactured"): ("Mica", "Unmanufactured"),
    ("Mica", "Waste"): None,
    ("Molybdenum", "Metal"): ("Molybdenum", "Refinery"),
    ("Molybdenum", "Ores & concentrates"): ("Molybdenum", "Ores & concentrates"),
    ("Molybdenum", "Oxides"): None,
    ("Molybdenum", "Scrap"): None,
    ("Molybdenum, mine", "Unknown"): ("Molybdenum", "Mine"),
    # NOTE: I removed natural gas, as the units are not clear: sometimes "million cubic meters" appears, sometimes no
    #  units are explicitly mentioned, and sometimes the notes mention oil equivalent.
    ("Natural gas", "Unknown"): None,
    ("Nepheline syenite", "Nepheline concentrates"): None,
    # NOTE: The following could be mapped to ("Nepheline syenite", "Mine"), but it has very sparse and noisy data for just a few countries.
    ("Nepheline syenite", "Nepheline-syenite"): None,
    # NOTE: The following could be mapped to ("Nepheline syenite", "Mine"), but it has very sparse and noisy data for just a few countries.
    ("Nepheline syenite", "Unknown"): None,
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
    ("Nickel, smelter/refinery", "Unknown"): ("Nickel", "Processing"),
    ("Perlite", "Unknown"): ("Perlite", "Mine"),
    ("Petroleum, crude", "Unknown"): ("Petroleum", "Crude"),
    ("Phosphate rock", "Aluminium phosphate"): ("Phosphate rock", "Mine, aluminum phosphate"),
    # NOTE: Old data.
    ("Phosphate rock", "Apatite"): None,
    ("Phosphate rock", "Calcium phosphates"): ("Phosphate rock", "Calcium phosphates"),
    ("Phosphate rock", "Guano"): ("Phosphate rock", "Guano"),
    ("Phosphate rock", "Unknown"): ("Phosphate rock", "Mine"),
    # NOTE: Only imports old data.
    ("Platinum group metals", "Iridium, osmium & ruthenium"): None,
    # NOTE: Only imports/exports old data.
    ("Platinum group metals", "Ores & concentrates"): None,
    ("Platinum group metals", "Other platinum group metals"): ("Platinum group metals", "Other platinum group metals"),
    ("Platinum group metals", "Other platinum metals"): ("Platinum group metals", "Other platinum metals"),
    ("Platinum group metals", "Palladium"): ("Platinum group metals", "Palladium"),
    ("Platinum group metals", "Platinum"): ("Platinum group metals", "Platinum"),
    ("Platinum group metals", "Platinum & platinum metals"): ("Platinum group metals", "Platinum & platinum metals"),
    # NOTE: Only imports/exports old data.
    ("Platinum group metals", "Platinum metals"): None,
    # NOTE: Only imports old data.
    ("Platinum group metals", "Rhodium"): None,
    # NOTE: Only imports old data.
    ("Platinum group metals", "Ruthenium"): None,
    # NOTE: Only imports old data.
    ("Platinum group metals", "Sponge"): None,
    ("Platinum group metals", "Waste & scrap"): None,
    ("Platinum group metals, mine", "Iridium"): ("Platinum group metals", "Mine, iridium"),
    # NOTE: Old data.
    ("Platinum group metals, mine", "Osmiridium"): None,
    # NOTE: Old data.
    ("Platinum group metals, mine", "Osmium"): None,
    ("Platinum group metals, mine", "Other platinum metals"): ("Platinum group metals", "Mine, other"),
    ("Platinum group metals, mine", "Palladium"): ("Platinum group metals", "Mine, palladium"),
    ("Platinum group metals, mine", "Platinum"): ("Platinum group metals", "Mine, platinum"),
    ("Platinum group metals, mine", "Rhodium"): ("Platinum group metals", "Mine, rhodium"),
    ("Platinum group metals, mine", "Ruthenium"): ("Platinum group metals", "Mine, ruthenium"),
    ("Platinum group metals, mine", "Unknown"): None,
    # NOTE: Only imports/exports old data.
    ("Potash", "Carbonate"): None,
    # NOTE: Only imports/exports old data.
    ("Potash", "Caustic potash"): None,
    # NOTE: Only imports/exports old data.
    ("Potash", "Chlorate"): None,
    ("Potash", "Chloride"): ("Potash", "Mine, chloride"),
    # NOTE: Only imports old data.
    ("Potash", "Cyanide"): None,
    # NOTE: Only imports/exports old data.
    ("Potash", "Fertiliser salts"): None,
    # NOTE: Only production old data.
    ("Potash", "Kainite, sylvinite"): None,
    # NOTE: Only data prior to 2002.
    ("Potash", "Nitrate"): None,
    # NOTE: Only production data prior to 1974.
    ("Potash", "Other fertiliser salts"): None,
    # NOTE: Only imports/exports old data.
    ("Potash", "Other potassic chemicals"): None,
    ("Potash", "Other potassic fertilisers"): None,
    ("Potash", "Polyhalite"): ("Potash", "Mine, polyhalite"),
    # NOTE: Only imports/exports old data.
    ("Potash", "Potassic chemicals"): None,
    ("Potash", "Potassic fertilisers"): None,
    ("Potash", "Potassic salts"): ("Potash", "Mine, potassic salts"),
    ("Potash", "Sulfide"): None,
    ("Potash", "Unknown"): ("Potash", "Unspecified"),
    ("Rare earth minerals", "Bastnaesite"): None,
    ("Rare earth minerals", "Loparite"): None,
    ("Rare earth minerals", "Monazite"): None,
    # NOTE: The following could possibly be mapped to ("Rare earths", "Mine, ores & concentrates"), but it has only sparse data for a few countries.
    ("Rare earth minerals", "Unknown"): None,
    ("Rare earth minerals", "Xenotime"): None,
    ("Rare earth oxides", "Unknown"): None,
    ("Rare earths", "Cerium compounds"): None,
    ("Rare earths", "Cerium metal"): None,
    ("Rare earths", "Ferro-cerium & other pyrophoric alloys"): None,
    ("Rare earths", "Metals"): ("Rare earths", "Refinery"),
    # NOTE: The following could possibly be mapped to ("Rare earths", "Mine, ores & concentrates"), but it possibly is only for imports/exports data.
    ("Rare earths", "Ores & concentrates"): None,
    ("Rare earths", "Other rare earth compounds"): None,
    ("Rare earths", "Rare earth compounds"): ("Rare earths", "Compounds"),
    ("Rhenium", "Unknown"): ("Rhenium", "Mine"),
    ("Salt", "Brine salt"): ("Salt", "Brine salt"),
    ("Salt", "Brine salt & sea salt"): None,
    # NOTE: Only available for imports before 1975.
    ("Salt", "Common salt"): None,
    # NOTE: Only available for imports before 1975.
    ("Salt", "Crude salt"): None,
    ("Salt", "Evaporated salt"): ("Salt", "Evaporated salt"),
    ("Salt", "Other salt"): ("Salt", "Other salt"),
    # NOTE: Only available for imports/exports before 1975.
    ("Salt", "Refined salt"): None,
    ("Salt", "Rock salt"): ("Salt", "Rock salt"),
    ("Salt", "Rock salt & brine salt"): None,
    ("Salt", "Salt in brine"): ("Salt", "Salt in brine"),
    ("Salt", "Sea salt"): ("Salt", "Sea salt"),
    # NOTE: Unclear what "Unknown" means, but it's significantly lower than USGS' "Mine".
    ("Salt", "Unknown"): None,
    ("Selenium, refined", "Unknown"): ("Selenium", "Refinery"),
    # NOTE: The following could be mapped to ("Andalusite", "Mine") (and so does USGS, which does not include global data).
    ("Sillimanite minerals", "Andalusite"): None,
    ("Sillimanite minerals", "Andalusite & kyanite"): None,
    # NOTE: The following could possibly be mapped to ("Kyanite", "Mine"), but it has very sparse data (and so does USGS).
    ("Sillimanite minerals", "Kyanite"): None,
    ("Sillimanite minerals", "Kyanite & related minerals"): None,
    ("Sillimanite minerals", "Mullite"): None,
    ("Sillimanite minerals", "Mullite, chamotte, dinas earth"): None,
    ("Sillimanite minerals", "Other"): None,
    # NOTE: The following could be mapped to ("Sillimanite", "Mine"), but it has very sparse data, and it's not included in USGS data.
    ("Sillimanite minerals", "Sillimanite"): None,
    ("Sillimanite minerals", "Sillimanite minerals"): None,
    ("Sillimanite minerals", "Sillimanite minerals & dinas earth"): None,
    ("Sillimanite minerals", "Sillimanite minerals, calcined"): None,
    ("Sillimanite minerals", "Sillimanite minerals, crude"): None,
    ("Sillimanite minerals", "Sillimanite minerals, crude & calcined"): None,
    ("Sillimanite minerals", "Sillimanite minerals, other"): None,
    ("Sillimanite minerals", "Sillimanite mins, chamotte, dinas earth"): None,
    ("Sillimanite minerals", "Unknown"): None,
    ("Silver", "Alloys"): None,
    ("Silver", "Metal"): ("Silver", "Metal"),
    ("Silver", "Metal, refined"): None,
    ("Silver", "Metal, unrefined"): None,
    ("Silver", "Ores & concentrates"): ("Silver", "Ores & concentrates"),
    ("Silver", "Silver-lead bullion"): None,
    ("Silver", "Unknown"): None,
    ("Silver", "Waste & scrap"): None,
    ("Silver, mine", "Unknown"): ("Silver", "Mine"),
    ("Sodium carbonate, natural", "Unknown"): ("Soda ash", "Natural"),
    ("Steel, crude", "Unknown"): ("Steel", "Processing, crude"),
    ("Strontium minerals", "Unknown"): ("Strontium", "Mine"),
    # NOTE: "Sulphur and pyrites" has only imports and exports data, no production.
    #  And most categories only have data in the past (e.g. before 1990).
    ("Sulphur and pyrites", "Other"): None,
    ("Sulphur and pyrites", "Precipitated"): None,
    ("Sulphur and pyrites", "Pyrites"): ("Sulfur", "Pyrites"),
    ("Sulphur and pyrites", "Pyrites - cupreous"): None,
    ("Sulphur and pyrites", "Pyrites - iron"): None,
    ("Sulphur and pyrites", "Sublimed"): None,
    ("Sulphur and pyrites", "Sulfur"): ("Sulfur", "Sulfur"),
    ("Sulphur and pyrites", "Sulfur, crude"): ("Sulfur", "Crude"),
    ("Sulphur and pyrites", "Sulfur, other"): None,
    ("Sulphur and pyrites", "Sulfur, refined"): ("Sulfur", "Refinery"),
    ("Sulphur and pyrites", "Sulfur, sublimed & precipitated"): None,
    ("Sulphur and pyrites", "Zinc concentrates"): None,
    # NOTE: Only production data prior to 2000.
    ("Talc", "Agalmatolite"): None,
    ("Talc", "Other"): None,
    ("Talc", "Pyrophyllite"): ("Talc and pyrophyllite", "Mine, pyrophyllite"),
    ("Talc", "Steatite"): None,
    # NOTE: Unclear what "Unknown" means, it doesn't agree with any of USGS categories.
    ("Talc", "Unknown"): None,
    # NOTE: Only production data prior to 1992.
    ("Talc", "Wonderstone (Pyrophyllite)"): None,
    ("Tantalum and niobium", "Columbite"): ("Coltan", "Mine, columbite"),
    ("Tantalum and niobium", "Columbite & tantalite"): ("Coltan", "Mine, columbite-tantalite"),
    ("Tantalum and niobium", "Columbite-tantalite"): ("Coltan", "Mine, columbite-tantalite"),
    ("Tantalum and niobium", "Niobium"): ("Niobium", "Mine"),
    ("Tantalum and niobium", "Niobium & tantalum ores"): None,
    ("Tantalum and niobium", "Niobium concentrates"): None,
    # NOTE: Only used for imports prior to 2002.
    ("Tantalum and niobium", "Niobium ores & concentrates"): None,
    ("Tantalum and niobium", "Pyrochlore"): ("Niobium", "Mine, pyrochlore"),
    ("Tantalum and niobium", "Tantalum"): ("Tantalum", "Mine"),
    ("Tantalum and niobium", "Tantalum concentrates"): None,
    ("Tantalum and niobium", "Tantalum ores & concentrates"): None,
    ("Tantalum and niobium", "Tin slags"): None,
    ("Tantalum and niobium", "Tin slags, Nb content"): None,
    ("Tantalum and niobium", "Tin slags, Ta content"): None,
    ("Tantalum and niobium", "Unknown"): None,
    ("Tantalum and niobium minerals", "Columbite"): ("Coltan", "Mine, columbite"),
    ("Tantalum and niobium minerals", "Columbite- Nb content"): None,
    ("Tantalum and niobium minerals", "Columbite- Ta content"): None,
    ("Tantalum and niobium minerals", "Columbite-tantalite"): ("Coltan", "Mine, columbite-tantalite"),
    ("Tantalum and niobium minerals", "Columbite-tantalite-Nb content"): None,
    ("Tantalum and niobium minerals", "Columbite-tantalite-Ta content"): None,
    ("Tantalum and niobium minerals", "Djalmaite"): None,
    ("Tantalum and niobium minerals", "Microlite"): None,
    ("Tantalum and niobium minerals", "Pyrochlore"): ("Niobium", "Mine, pyrochlore"),
    ("Tantalum and niobium minerals", "Pyrochlore -Nb content"): None,
    ("Tantalum and niobium minerals", "Struverite"): None,
    ("Tantalum and niobium minerals", "Struverite (Ta content)"): None,
    ("Tantalum and niobium minerals", "Tantalite"): ("Coltan", "Mine, tantalite"),
    ("Tantalum and niobium minerals", "Tantalite -Ta content"): None,
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Nb content)"): None,
    ("Tantalum and niobium minerals", "Tantalum & Niobium (Ta content)"): None,
    # NOTE: The following could be mapped to ("Tellurium", "Refinery"). However, we decided to discard Tellurium.
    ("Tellurium, refined", "Unknown"): None,
    ("Tin", "Concentrates"): None,
    ("Tin", "Scrap"): None,
    ("Tin", "Tin-silver ore"): None,
    ("Tin", "Unwrought"): None,
    ("Tin", "Unwrought & scrap"): None,
    ("Tin", "Unwrought & semi-manufactures"): None,
    ("Tin", "Unwrought alloys"): None,
    ("Tin, mine", "Unknown"): ("Tin", "Mine"),
    ("Tin, smelter", "Unknown"): ("Tin", "Smelter"),
    ("Titanium", "Ilmenite"): ("Titanium", "Mine, ilmenite"),
    ("Titanium", "Leucoxene"): None,
    ("Titanium", "Metal"): ("Titanium", "Metal"),
    # NOTE: Titanium ores & concentrates only has exports data between 1970 and 1975.
    ("Titanium", "Ores & concentrates"): None,
    ("Titanium", "Other titanium minerals"): None,
    ("Titanium", "Oxides"): None,
    ("Titanium", "Rutile"): ("Titanium", "Mine, rutile"),
    ("Titanium", "Rutile sand"): None,
    ("Titanium", "Sponge"): None,
    ("Titanium", "Titanium minerals"): ("Titanium", "Titanium minerals"),
    ("Titanium", "Titanium minerals & slag"): None,
    ("Titanium", "Titanium slag"): None,
    ("Titanium", "Unwrought"): None,
    ("Titanium", "Wrought"): None,
    ("Titanium minerals", "Ilmenite"): ("Titanium", "Mine, ilmenite"),
    ("Titanium minerals", "Leucoxene"): None,
    ("Titanium minerals", "Rutile"): ("Titanium", "Mine, rutile"),
    ("Titanium minerals", "Titanium slag"): None,
    ("Titanium minerals", "Unknown"): None,
    # NOTE: The following is for imports/exports data prior to 2003.
    ("Tungsten", "Ammonium paratungstate"): None,
    ("Tungsten", "Carbide"): ("Tungsten", "Carbide"),
    ("Tungsten", "Metal"): ("Tungsten", "Metal"),
    ("Tungsten", "Ores & concentrates"): ("Tungsten", "Ores & concentrates"),
    # NOTE: The following is for exports data prior to 1980.
    ("Tungsten", "Other tungsten ores"): None,
    # NOTE: The following is for exports data prior to 1972.
    ("Tungsten", "Powder"): None,
    # NOTE: The following is for exports data prior to 1990.
    ("Tungsten", "Scheelite ores & concentrates"): None,
    # NOTE: The following is for exports data prior to 1990.
    ("Tungsten", "Unknown"): None,
    # NOTE: The following is for exports data prior to 1990.
    ("Tungsten", "Wolframite ores & concentrates"): None,
    # NOTE: The following is in good agreement with USGS production data.
    ("Tungsten, mine", "Unknown"): ("Tungsten", "Mine"),
    ("Uranium", "Unknown"): ("Uranium", "Mine"),
    # NOTE: Only exports data prior to 1979.
    ("Vanadium", "Lead vanadium concentrates"): None,
    ("Vanadium", "Metal"): ("Vanadium", "Metal"),
    # NOTE: Only imports/exports data prior to 2010.
    ("Vanadium", "Ores & concentrates"): None,
    ("Vanadium", "Pentoxide"): ("Vanadium", "Pentoxide"),
    ("Vanadium", "Vanadiferous residues"): ("Vanadium", "Vanadiferous residues"),
    ("Vanadium", "Vanadium-titanium pig iron"): None,
    ("Vanadium, mine", "Unknown"): ("Vanadium", "Mine"),
    # NOTE: The following could be mapped to ("Vermiculite", "Mine"). However, we decided to discard Vemiculite.
    ("Vermiculite", "Unknown"): None,
    # NOTE: The following could be mapped to ("Wollastonite", "Mine"). However, we decided to discard Wollastonite.
    ("Wollastonite", "Unknown"): None,
    ("Zinc", "Crude & refined"): None,
    ("Zinc", "Ores & concentrates"): None,
    ("Zinc", "Oxides"): None,
    ("Zinc", "Scrap"): None,
    ("Zinc", "Unwrought"): None,
    ("Zinc", "Unwrought alloys"): None,
    ("Zinc, mine", "Unknown"): ("Zinc", "Mine"),
    ("Zinc, slab", "Unknown"): ("Zinc", "Refinery"),
    ("Zirconium", "Concentrates"): ("Zirconium and hafnium", "Concentrates"),
    ("Zirconium", "Metal"): ("Zirconium and hafnium", "Metal"),
    # NOTE: Only imports/exports old data.
    ("Zirconium", "Unknown"): None,
    # NOTE: Only imports/exports data prior to 2003.
    ("Zirconium", "Zirconium sand"): None,
    ("Zirconium minerals", "Unknown"): ("Zirconium and hafnium", "Mine"),
}

# Mapping from original unit names to tonnes.
# NOTE: The keys in this dictionary should coincide with all units found in the data.
UNIT_MAPPING = {
    "tonnes": "tonnes",
    "tonnes (metric)": "tonnes",
    # "tonnes (Al2O3 content)": "tonnes of aluminum oxide content",
    # "tonnes (K20 content)": "tonnes of potassium oxide content",
    # "tonnes (metal content)": "tonnes of metal content",
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
    "Alumina",
    "Antimony",
    "Cement",
    "Cobalt",
    "Copper",
    "Lead",
    "Molybdenum",
    "Nickel",
    "Silver",
    "Tin",
    "Zinc",
    "Tungsten",
    "Vanadium",
    "Potash",
    "Platinum group metals",
    "Uranium",
    "Bismuth",
]

# Footnotes (that will appear in the footer of charts) to add to the flattened output table.
FOOTNOTES = {
    "production|Antimony|Mine|tonnes": "Values are reported as tonnes of metal content.",
    "production|Potash|Mine, chloride|tonnes": "Values are reported as tonnes of potassium oxide content.",
    "production|Potash|Unspecified|tonnes": "Values are reported as tonnes of potassium oxide content.",
    "production|Potash|Mine, polyhalite|tonnes": "Values are reported as tonnes of potassium oxide content.",
    "production|Potash|Mine, potassic salts|tonnes": "Values are reported as tonnes of potassium oxide content.",
    # "imports|Potash|Mine, chloride|tonnes": "Values are reported as tonnes of potassium oxide content.",
    # "exports|Potash|Mine, chloride|tonnes": "Values are reported as tonnes of potassium oxide content.",
    "production|Platinum group metals|Mine, iridium|tonnes": "Values are reported as tonnes of metal content.",
    "production|Platinum group metals|Mine, other|tonnes": "Values are reported as tonnes of metal content.",
    "production|Platinum group metals|Mine, palladium|tonnes": "Values are reported as tonnes of metal content.",
    "production|Platinum group metals|Mine, platinum|tonnes": "Values are reported as tonnes of metal content.",
    "production|Platinum group metals|Mine, rhodium|tonnes": "Values are reported as tonnes of metal content.",
    "production|Uranium|Mine|tonnes": "Values are reported as tonnes of metal content.",
    "production|Bismuth|Mine|tonnes": "Values are reported as tonnes of metal content.",
}

# There are many historical regions with overlapping data with their successor countries.
# Accept only overlaps on the year when the historical country stopped existing.
# NOTE: We decided to not include region aggregates, but this is still relevant because, to create world data, we first
#  create data for continents, then build an aggregate for the world, and then remove continents.
#  World data aggregated in this step will be used in the garden minerals step to compare it with the World data given
#  by USGS. But the World data created in this step will then be removed and not shown in the minerals explorer.
# NOTE: Some of the overlaps occur only for certain commodities.
ACCEPTED_OVERLAPS = [
    # {1991: {"USSR", "Armenia"}},
    # {1991: {"USSR", "Belarus"}},
    # {1991: {"USSR", "Russia"}},
    {1992: {"Czechia", "Czechoslovakia"}},
    {1992: {"Slovakia", "Czechoslovakia"}},
    # {1990: {"Germany", "East Germany"}},
    # {1990: {"Germany", "West Germany"}},
    # {2010: {"Netherlands Antilles", "Bonaire Sint Eustatius and Saba"}},
    # {1990: {"Yemen", "Yemen People's Republic"}},
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
        "category": [],
        "commodity": [],
        "sub_commodity": [],
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


def gather_notes(
    tb: Table, notes_columns: List[str], notes_original: Dict[str, List[str]], notes_edited: Dict[str, List[str]]
) -> Dict[str, str]:
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
    for column in tqdm(tb_flat_notes.drop(columns=["country", "year"]).columns, disable=True):
        _notes = tb_flat_notes[column].dropna().tolist()
        if len(_notes) > 0:
            # Gather all notes for this column.
            notes = sum(_notes, [])
            # Get unique notes keeping the order.
            notes = pd.unique(pd.Series(notes)).tolist()
            # Join notes.
            if len(notes) > 0:
                notes_dict[column] = notes

    # Check that the notes coincide with the original notes stored in an adjacent file.
    error = "Original BGS notes and footnotes have changed."
    assert notes_dict == notes_original, error
    # To update the original notes:
    # from etl.files import ruamel_dump
    # (paths.directory / "notes_original.yml").write_text(ruamel_dump(notes_original))

    # Load the edited notes, that will overwrite the original notes.
    notes_dict.update(notes_edited)

    # Join all notes into one string, separated by line breaks.
    notes_str_dict = {}
    for column, notes in notes_dict.items():
        notes_str_dict[column] = "- " + "\n- ".join(notes)

    return notes_str_dict


def add_global_data(tb: Table, ds_regions: Dataset) -> Table:
    # We want to create a "World" aggregate.
    # This is useful to later inspect how BGS global data compares to USGS.
    # For now other regions are not important (since the data is very sparse, and
    # therefore aggregtes will not be representative of the region).
    # We could simply add up all countries, but we need to be aware of possible region overlaps.
    # Therefore, I will use geo.add_regions_to_table to create all regions.
    # It will also create an aggregate for "World" (which will be created by aggregating newly created continent
    # aggregates).
    # Then I will remove all other region aggregates.
    regions = {
        "Africa": {},
        "Asia": {},
        "Europe": {},
        "North America": {},
        "Oceania": {},
        "South America": {},
        "World": {},
    }
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
        index_columns=["country", "year", "commodity", "sub_commodity", "unit"],
        accepted_overlaps=ACCEPTED_OVERLAPS,
    )
    # Now that we have a World aggregate (and we are sure there is no double-counting) remove all other regions.
    regions_to_remove = [region for region in regions if region != "World"]
    tb = tb.loc[~tb["country"].isin(regions_to_remove)].reset_index(drop=True)

    # # We noticed that imports/exports data have:
    # # * Only data for European countries (and Turkey) from 2003 onwards. Check this:
    # regions = ds_regions["regions"]
    # europe = regions.loc[json.loads(regions[regions["name"] == "Europe"]["members"].item())]["name"].unique().tolist()
    # error = "Expected only European countries (including Turkey) imports/exports data after 2002."
    # assert set(tb[(tb["imports"].notnull()) & (tb["year"] > 2002)]["country"]) <= (
    #     set(europe) | set(["United Kingdom", "Turkey", "World"])
    # ), error
    # assert set(tb[(tb["exports"].notnull()) & (tb["year"] > 2002)]["country"]) <= (
    #     set(europe) | set(["United Kingdom", "Turkey", "World"])
    # ), error
    # # * Only UK data from 2019 onwards. Check this:
    # error = "Expected only UK imports/exports data after 2018."
    # assert set(tb[(tb["imports"].notnull()) & (tb["year"] > 2018)]["country"]) == set(
    #     ["United Kingdom", "World"]
    # ), error
    # assert set(tb[(tb["exports"].notnull()) & (tb["year"] > 2018)]["country"]) == set(
    #     ["United Kingdom", "World"]
    # ), error
    # # Therefore, it only makes sense to have global imports/exports data until 2002.
    # tb.loc[(tb["year"] > 2002) & (tb["country"] == "World"), ["imports", "exports"]] = None

    return tb


def aggregate_coal(tb: Table) -> Table:
    tb = tb.copy()

    # Check that categories like "Mine, brown coal & lignite" are not the combination of "Mine, brown coal" and "Mine, lignite".
    # Indeed, such pairs never overlap. So, we can safely add up all coal contributions into one total category.

    # import plotly.express as px
    # coal_agg = tb[(tb["category"]=="Production")&(tb["commodity"]=="Coal")].groupby(["country", "year"], observed=True, as_index=False).agg({"value": "sum"})
    # for country in sorted(set(coal_agg["country"])):
    #     _coal_disagg = tb[(tb["country"]==country)&(tb["category"]=="Production")&(tb["commodity"]=="Coal")]
    #     _coal_agg = coal_agg[coal_agg["country"]==country].assign(**{"sub_commodity": "SUM"})
    #     compare = pd.concat([_coal_disagg, _coal_agg], ignore_index=True)
    #     px.line(compare, x="year", y="value", color="sub_commodity", markers=True, title=country).show()

    # Select coal production data.
    tb_coal = tb[(tb["category"] == "Production") & (tb["commodity"] == "Coal")]

    # Create a series with the sum of all coal data per country-year.
    tb_coal_sum = tb_coal.groupby(["country", "year"], observed=True, as_index=False).agg({"value": "sum"})

    # Visually compare the resulting series with the one from the Statistical Review of World Energy.
    # from etl.paths import DATA_DIR
    # tb_sr = Dataset(DATA_DIR / "garden/energy_institute/2024-06-20/statistical_review_of_world_energy").read("statistical_review_of_world_energy")
    # tb_sr = tb_sr[["country", "year", 'coal_production_mt']].rename(columns={"coal_production_mt": "value"})
    # tb_sr["value"] *= 1e6
    # compare = pr.concat([tb_sr.assign(**{"source": "EI"}), tb_coal_sum.assign(**{"source": "BGS"})], ignore_index=True)
    # for country in sorted(set(compare["country"])):
    #     _compare = compare[compare["country"] == country]
    #     if len(_compare["source"].unique()) == 2:
    #         px.line(compare[compare["country"] == country], x="year", y="value", color="source", markers=True, title=country).show()

    # Concatenate the old data (removing the disaggregated coal data) with the aggregated coal data.
    tb_coal_sum = tb_coal_sum.assign(
        **{"category": "Production", "commodity": "Coal", "sub_commodity": "Mine", "unit": "tonnes"}
    )
    tb = pr.concat([tb.drop(tb_coal.index), tb_coal_sum], ignore_index=True)

    ####################################################################################################################
    # Remove some spurious values after visual inspection (comparing with the Statistical Review).
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Germany")
        & (tb["year"] == 1992)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Czechia")
        & (tb["year"] == 1992)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Slovakia")
        & (tb["year"] == 1992)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Mexico")
        & (tb["year"] == 2019)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Pakistan")
        & (tb["year"] == 1997)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "United Kingdom")
        & (tb["year"] == 1986)
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    # For Russia, 2018, 2019, 2020 and 2021 are much higher than the rest, and then 2022 drops in BGS data.
    # This does not happen in the Statistical Review, so it looks spurious.
    tb.loc[
        (tb["category"] == "Production")
        & (tb["country"] == "Russia")
        & (tb["year"].isin([2018, 2019, 2020, 2021, 2022]))
        & (tb["commodity"] == "Coal"),
        "value",
    ] = None
    ####################################################################################################################

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("world_mineral_statistics")
    tb = ds_meadow.read("world_mineral_statistics", safe_types=False)

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load adjacent file containing the original BGS notes and footnotes for each data column.
    # NOTE: This file is loaded as a sanity check, in case in a later update notes change.
    notes_original = ruamel_load(paths.directory / "notes_original.yml")

    # Load the addjacent file containing the edited notes.
    notes_edited = ruamel_load(paths.directory / "notes_edited.yml")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # We decided to discard imports and exports data, since (as mentioned in other comments) it includes data for
    # non-european countries only until 2002, and it causes many issues.
    tb = tb[tb["category"] == "Production"].reset_index(drop=True)

    # Remove data for regions that did not exist at the time.
    tb = remove_data_from_non_existing_regions(tb=tb)

    # Improve the name of the commodities.
    tb["commodity"] = tb["commodity"].str.capitalize()

    # Harmonize commodity-subcommodity pairs.
    tb = harmonize_commodity_subcommodity_pairs(tb=tb)

    # Harmonize units.
    tb = harmonize_units(tb=tb)

    ####################################################################################################################
    # Fix some known issues in the data.
    # Molybdenum, mine for the US between 1970 and 1976 is significantly larger than in USGS.
    # And BGS notes on 1977 say "Break in series". So I will remove those points prior to 1977.
    # A similar thing happens for Turkey and the USSR.
    # Even after removing these, BGS data is larger than USGS' World. So, simply remove all points prior to 1977.
    tb.loc[
        (tb["commodity"] == "Molybdenum")
        # & (tb["country"].isin(["United States", "Turkey", "USSR"]))
        & (tb["year"] < 1977)
        & (tb["category"] == "Production"),
        "value",
    ] = None

    # A similar issue happens with tugsten.
    tb.loc[
        (tb["commodity"] == "Tungsten") & (tb["year"] < 1977) & (tb["category"] == "Production"),
        "value",
    ] = None

    # Chromium in Colombia has spurious jumps and zeros in the 1970s.
    # Looking at the World Mineral Statistics:
    # https://www.bgs.ac.uk/mineralsuk/statistics/world-mineral-statistics/world-mineral-statistics-archive/
    # It seems that the zero may be spurious (not found in any of those PDFs) and all values are estimated.
    # I'll remove them.
    tb.loc[
        (tb["commodity"] == "Chromium")
        & (tb["sub_commodity"] == "Mine, gross weight")
        & (tb["country"] == "Colombia")
        & (tb["year"] <= 1976),
        "value",
    ] = None

    ####################################################################################################################

    # Combine all subcommodities of coal production, and fix some issues.
    tb = aggregate_coal(tb=tb)

    # Pivot table to have a column for each category.
    tb = (
        tb.pivot(
            index=["country", "year", "commodity", "sub_commodity", "unit"],
            columns="category",
            values=["value", "note", "general_notes"],
            join_column_levels_with="_",
        )
        .underscore()
        .rename(columns={"value_production": "production"}, errors="raise")
    )

    # Set an appropriate format for value columns.
    tb = tb.astype({column: "Float64" for column in ["production"]})

    # Parse notes as lists of strings.
    for column in [
        "note_production",
        "general_notes_production",
    ]:
        tb[column] = tb[column].fillna("[]").apply(ast.literal_eval)

    # Add global data.
    tb = add_global_data(tb=tb, ds_regions=ds_regions)

    # Clean notes columns, and combine notes at the individual row level with general table notes.
    for category in ["production"]:
        tb[f"notes_{category}"] = [
            clean_notes(note) for note in tb[f"note_{category}"] + tb[f"general_notes_{category}"]
        ]
        # Drop unnecessary columns.
        tb = tb.drop(columns=[f"note_{category}", f"general_notes_{category}"])

    # Gather all notes in a dictionary.
    notes = gather_notes(
        tb, notes_columns=["notes_production"], notes_original=notes_original, notes_edited=notes_edited
    )

    # Create a wide table.
    tb_flat = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["production"],
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
    for column in ["notes_production"]:
        tb[column] = tb[column].copy_metadata(tb["production"]).astype(str)

    # Add footnotes.
    for column, note in FOOTNOTES.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    # Drop empty columns.
    tb_flat = tb_flat.dropna(axis=1, how="all").reset_index(drop=True)

    # Format table conveniently.
    tb = tb.format(["country", "year", "commodity", "sub_commodity"])
    tb_flat = tb_flat.format(["country", "year"], short_name=paths.short_name + "_flat")
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns})

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_flat], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
