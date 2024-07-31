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
    ("Aggregates, primary", "Crushed rock"): ("Primary aggregates", "Crushed rock"),
    ("Aggregates, primary", "Sand and gravel"): ("Primary aggregates", "Sand and gravel"),
    ("Aggregates, primary", "Unknown"): ("Primary aggregates", "Unknown"),
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
    ("Antimony, mine", "Unknown"): ("Antimony", "Mine production"),
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
    ("Bismuth, mine", "Unknown"): ("Bismuth", "Mine production"),
    ("Borates", "Unknown"): ("Borates", "Unknown"),
    ("Bromine", "Compounds"): ("Bromine", "Compounds"),
    ("Bromine", "Unknown"): ("Bromine", "Unknown"),
    ("Cadmium", "Metal"): ("Cadmium", "Metal"),
    ("Cadmium", "Other"): ("Cadmium", "Other"),
    ("Cadmium", "Oxide"): ("Cadmium", "Oxide"),
    ("Cadmium", "Sulfide"): ("Cadmium", "Sulfide"),
    ("Cadmium", "Unknown"): ("Cadmium", "Unknown"),
    ("Cement", "Cement clinkers"): ("Cement", "Cement, clinker"),
    ("Cement", "Other cement"): ("Cement", "Other cement"),
    ("Cement", "Portland cement"): ("Cement", "Portland cement"),
    ("Cement  clinker", "Cement, clinker"): ("Cement", "Cement, clinker"),
    ("Cement, finished", "Cement, finished"): ("Cement", "Cement, finished"),
    ("Chromium", "Metal"): ("Chromium", "Metal"),
    ("Chromium", "Ores & concentrates"): ("Chromium", "Ores & concentrates"),
    ("Chromium ores and concentrates", "Unknown"): ("Chromium", "Ores & concentrates"),
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
    ("Cobalt", "Metal & refined"): ("Cobalt", "Metal & refined"),
    ("Cobalt", "Ore"): ("Cobalt", "Ore"),
    ("Cobalt", "Oxide, sinter & sulfide"): ("Cobalt", "Oxide, sinter & sulfide"),
    ("Cobalt", "Oxides"): ("Cobalt", "Oxides"),
    ("Cobalt", "Salts"): ("Cobalt", "Salts"),
    ("Cobalt", "Scrap"): ("Cobalt", "Scrap"),
    ("Cobalt", "Unknown"): ("Cobalt", "Unknown"),
    ("Cobalt", "Unwrought"): ("Cobalt", "Unwrought"),
    ("Cobalt, mine", "Unknown"): ("Cobalt", "Mine production"),
    ("Cobalt, refined", "Unknown"): ("Cobalt", "Refinery production"),
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
    ("Copper", "Unknown"): ("Copper", "Unknown"),
    ("Copper", "Unwrought"): ("Copper", "Unwrought"),
    ("Copper", "Unwrought & Scrap"): ("Copper", "Unwrought & Scrap"),
    ("Copper", "Unwrought alloys"): ("Copper", "Unwrought alloys"),
    ("Copper", "Unwrought, matte, cement & refined"): ("Copper", "Unwrought, matte, cement & refined"),
    ("Copper", "Unwrought, refined"): ("Copper", "Unwrought, refined"),
    ("Copper", "Unwrought, unrefined"): ("Copper", "Unwrought, unrefined"),
    ("Copper, mine", "Unknown"): ("Copper", "Mine production"),
    ("Copper, refined", "Unknown"): ("Copper", "Refinery production"),
    ("Copper, smelter", "Unknown"): ("Copper", "Smelter production"),
    # NOTE: It's unclear when synthetic diamond is included.
    # Production does not seem to include it, but imports and exports of "Dust" does include synthetic diamond.
    ("Diamond", "Cut"): ("Diamond", "Cut"),
    ("Diamond", "Dust"): ("Diamond", "Dust"),
    ("Diamond", "Gem"): ("Diamond", "Gem"),
    ("Diamond", "Gem, cut"): ("Diamond", "Gem, cut"),
    ("Diamond", "Gem, rough"): ("Diamond", "Gem, rough"),
    ("Diamond", "Industrial"): ("Diamond", "Mine production, industrial"),
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
    ("Ferro-alloys", "Ferro-aluminium"): ("Ferro-alloys", "Ferro-aluminum"),
    ("Ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): (
        "Ferro-alloys",
        "Ferro-aluminum & ferro-silico-aluminum",
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
    ("Ferro-alloys", "Ferro-silico-calcium-aluminium"): ("Ferro-alloys", "Ferro-silico-calcium-aluminum"),
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
    ("Fluorspar", "Unknown"): ("Fluorspar", "Unknown"),
    ("Gallium, primary", "Unknown"): ("Gallium", "Primary"),
    ("Gemstones", "Unknown"): ("Gemstones", "Unknown"),
    ("Germanium metal", "Unknown"): ("Germanium", "Metal"),
    ("Gold", "Metal"): ("Gold", "Metal"),
    ("Gold", "Metal, other"): ("Gold", "Metal, other"),
    ("Gold", "Metal, refined"): ("Gold", "Metal, refined"),
    ("Gold", "Metal, unrefined"): ("Gold", "Metal, unrefined"),
    ("Gold", "Ores & concentrates"): ("Gold", "Ores & concentrates"),
    ("Gold", "Ores, concentrates & unrefined metal"): ("Gold", "Ores, concentrates & unrefined metal"),
    ("Gold", "Waste & scrap"): ("Gold", "Waste & scrap"),
    ("Gold, mine", "Unknown"): ("Gold", "Mine production"),
    ("Graphite", "Unknown"): ("Graphite", "Unknown"),
    ("Gypsum and plaster", "Anhydrite"): ("Gypsum and plaster", "Anhydrite"),
    ("Gypsum and plaster", "Calcined"): ("Gypsum and plaster", "Calcined"),
    ("Gypsum and plaster", "Crede & ground"): ("Gypsum and plaster", "Crede & ground"),
    ("Gypsum and plaster", "Crude"): ("Gypsum and plaster", "Crude"),
    ("Gypsum and plaster", "Crude & calcined"): ("Gypsum and plaster", "Crude & calcined"),
    ("Gypsum and plaster", "Ground & calcined"): ("Gypsum and plaster", "Ground & calcined"),
    ("Gypsum and plaster", "Unknown"): ("Gypsum and plaster", "Unknown"),
    ("Helium", "Helium"): ("Helium", "Helium"),
    ("Indium, refinery", "Unknown"): ("Indium, refinery", "Unknown"),
    ("Iodine", "Unknown"): ("Iodine", "Unknown"),
    ("Iron ore", "Burnt pyrites"): ("Iron ore", "Burnt pyrites"),
    ("Iron ore", "Unknown"): ("Iron ore", "Unknown"),
    ("Iron, pig", "Unknown"): ("Iron, pig", "Unknown"),
    ("Iron, steel and ferro-alloys", "Fe-silico-spiegeleisen & Si-Mn"): (
        "Iron, steel and ferro-alloys",
        "Fe-silico-spiegeleisen & Si-Mn",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-Si-manganese & silico-speigeleisen"): (
        "Iron, steel and ferro-alloys",
        "Ferro-Si-manganese & silico-speigeleisen",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-alloys"): ("Iron, steel and ferro-alloys", "Ferro-alloys"),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium"): ("Iron, steel and ferro-alloys", "Ferro-aluminum"),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium & ferro-silico-aluminium"): (
        "Iron, steel and ferro-alloys",
        "Ferro-aluminum & ferro-silico-aluminum",
    ),
    ("Iron, steel and ferro-alloys", "Ferro-aluminium, Fe-Si-Al & Fe-Si-Mn-Al"): (
        "Iron, steel and ferro-alloys",
        "Ferro-aluminum, Fe-Si-Al & Fe-Si-Mn-Al",
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
        "Ferro-silico-aluminum, Fe-Si-Mn-Al, Fe-Si-Al-Ca",
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
        "Ferro-silico-manganese-aluminum",
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
    ("Kaolin", "Unknown"): ("Kaolin", "Unknown"),
    ("Lead", "Ores & concentrates"): ("Lead", "Ores & concentrates"),
    ("Lead", "Refined"): ("Lead", "Refined"),
    ("Lead", "Scrap"): ("Lead", "Scrap"),
    ("Lead", "Unwrought"): ("Lead", "Unwrought"),
    ("Lead", "Unwrought & scrap"): ("Lead", "Unwrought & scrap"),
    ("Lead", "Unwrought & semi-manufactures"): ("Lead", "Unwrought & semi-manufactures"),
    ("Lead", "Unwrought alloys"): ("Lead", "Unwrought alloys"),
    ("Lead, mine", "Unknown"): ("Lead", "Mine production"),
    ("Lead, refined", "Unknown"): ("Lead, refined", "Unknown"),
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
    ("Lithium minerals", "Unknown"): ("Lithium minerals", "Unknown"),
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
    ("Manganese", "Metal"): ("Manganese", "Metal"),
    ("Manganese", "Ores & Concentrates"): ("Manganese", "Ores & Concentrates"),
    ("Manganese ore", "Chemical"): ("Manganese ore", "Chemical"),
    ("Manganese ore", "Manganese ore (ferruginous)"): ("Manganese ore", "Manganese ore (ferruginous)"),
    ("Manganese ore", "Metallurgical"): ("Manganese ore", "Metallurgical"),
    ("Manganese ore", "Unknown"): ("Manganese ore", "Unknown"),
    ("Mercury", "Unknown"): ("Mercury", "Unknown"),
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
    ("Molybdenum", "Metal"): ("Molybdenum", "Metal"),
    ("Molybdenum", "Ores & concentrates"): ("Molybdenum", "Ores & concentrates"),
    ("Molybdenum", "Oxides"): ("Molybdenum", "Oxides"),
    ("Molybdenum", "Scrap"): ("Molybdenum", "Scrap"),
    ("Molybdenum, mine", "Unknown"): ("Molybdenum", "Mine production"),
    # NOTE: I removed natural gas, as the units are not clear: sometimes "million cubic meters" appears, sometimes no
    #  units are explicitly mentioned, and sometimes the notes mention oil equivalent.
    ("Natural gas", "Unknown"): None,
    ("Nepheline syenite", "Nepheline concentrates"): ("Nepheline syenite", "Nepheline concentrates"),
    ("Nepheline syenite", "Nepheline-syenite"): ("Nepheline syenite", "Nepheline-syenite"),
    ("Nepheline syenite", "Unknown"): ("Nepheline syenite", "Unknown"),
    ("Nickel", "Mattes, sinters etc"): ("Nickel", "Mattes, sinters etc"),
    ("Nickel", "Ores & concentrates"): ("Nickel", "Ores & concentrates"),
    ("Nickel", "Ores, concentrates & scrap"): ("Nickel", "Ores, concentrates & scrap"),
    ("Nickel", "Ores, concentrates, mattes etc"): ("Nickel", "Ores, concentrates, mattes etc"),
    ("Nickel", "Oxide, sinter & sulfide"): ("Nickel", "Oxide, sinter & sulfide"),
    ("Nickel", "Oxides"): ("Nickel", "Oxides"),
    ("Nickel", "Scrap"): ("Nickel", "Scrap"),
    ("Nickel", "Slurry, mattes, sinters etc"): ("Nickel", "Slurry, mattes, sinters etc"),
    ("Nickel", "Sulfide"): ("Nickel", "Sulfide"),
    ("Nickel", "Unknown"): ("Nickel", "Unknown"),
    ("Nickel", "Unwrought"): ("Nickel", "Unwrought"),
    ("Nickel", "Unwrought alloys"): ("Nickel", "Unwrought alloys"),
    ("Nickel", "Unwrought, mattes, sinters etc"): ("Nickel", "Unwrought, mattes, sinters etc"),
    ("Nickel, mine", "Unknown"): ("Nickel", "Mine production"),
    ("Nickel, smelter/refinery", "Sulfate"): ("Nickel", "Smelter or refinery, sulfate"),
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
    ("Platinum group metals, mine", "Iridium"): ("Platinum group metals", "Mine production, iridium"),
    ("Platinum group metals, mine", "Osmiridium"): ("Platinum group metals", "Mine production, osmiridium"),
    ("Platinum group metals, mine", "Osmium"): ("Platinum group metals", "Mine production, osmium"),
    ("Platinum group metals, mine", "Other platinum metals"): ("Platinum group metals", "Mine production, other"),
    ("Platinum group metals, mine", "Palladium"): ("Platinum group metals", "Mine production, palladium"),
    ("Platinum group metals, mine", "Platinum"): ("Platinum group metals", "Mine production, platinum"),
    ("Platinum group metals, mine", "Rhodium"): ("Platinum group metals", "Mine production, rhodium"),
    ("Platinum group metals, mine", "Ruthenium"): ("Platinum group metals", "Mine production, ruthenium"),
    ("Platinum group metals, mine", "Unknown"): ("Platinum group metals", "Mine production, unknown"),
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
    ("Rare earth minerals", "Bastnaesite"): ("Rare earth minerals", "Bastnaesite"),
    ("Rare earth minerals", "Loparite"): ("Rare earth minerals", "Loparite"),
    ("Rare earth minerals", "Monazite"): ("Rare earth minerals", "Monazite"),
    ("Rare earth minerals", "Unknown"): ("Rare earth minerals", "Unknown"),
    ("Rare earth minerals", "Xenotime"): ("Rare earth minerals", "Xenotime"),
    ("Rare earth oxides", "Unknown"): ("Rare earth oxides", "Unknown"),
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
    ("Selenium, refined", "Unknown"): ("Selenium, refined", "Unknown"),
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
    ("Silver", "Alloys"): ("Silver", "Alloys"),
    ("Silver", "Metal"): ("Silver", "Metal"),
    ("Silver", "Metal, refined"): ("Silver", "Metal, refined"),
    ("Silver", "Metal, unrefined"): ("Silver", "Metal, unrefined"),
    ("Silver", "Ores & concentrates"): ("Silver", "Ores & concentrates"),
    ("Silver", "Silver-lead bullion"): ("Silver", "Silver-lead bullion"),
    ("Silver", "Unknown"): ("Silver", "Unknown"),
    ("Silver", "Waste & scrap"): ("Silver", "Waste & scrap"),
    ("Silver, mine", "Unknown"): ("Silver", "Mine production"),
    ("Sodium carbonate, natural", "Unknown"): ("Sodium carbonate", "Natural"),
    ("Steel, crude", "Unknown"): ("Steel", "Crude"),
    ("Strontium minerals", "Unknown"): ("Strontium minerals", "Unknown"),
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
    ("Tellurium, refined", "Unknown"): ("Tellurium", "Refined"),
    ("Tin", "Concentrates"): ("Tin", "Concentrates"),
    ("Tin", "Scrap"): ("Tin", "Scrap"),
    ("Tin", "Tin-silver ore"): ("Tin", "Tin-silver ore"),
    ("Tin", "Unwrought"): ("Tin", "Unwrought"),
    ("Tin", "Unwrought & scrap"): ("Tin", "Unwrought & scrap"),
    ("Tin", "Unwrought & semi-manufactures"): ("Tin", "Unwrought & semi-manufactures"),
    ("Tin", "Unwrought alloys"): ("Tin", "Unwrought alloys"),
    ("Tin, mine", "Unknown"): ("Tin", "Mine production"),
    ("Tin, smelter", "Unknown"): ("Tin", "Smelter production"),
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
    ("Tungsten, mine", "Unknown"): ("Tungsten", "Mine production"),
    ("Uranium", "Unknown"): ("Uranium", "Unknown"),
    ("Vanadium", "Lead vanadium concentrates"): ("Vanadium", "Lead vanadium concentrates"),
    ("Vanadium", "Metal"): ("Vanadium", "Metal"),
    ("Vanadium", "Ores & concentrates"): ("Vanadium", "Ores & concentrates"),
    ("Vanadium", "Pentoxide"): ("Vanadium", "Pentoxide"),
    ("Vanadium", "Vanadiferous residues"): ("Vanadium", "Vanadiferous residues"),
    ("Vanadium", "Vanadium-titanium pig iron"): ("Vanadium", "Vanadium-titanium pig iron"),
    ("Vanadium, mine", "Unknown"): ("Vanadium", "Mine production"),
    ("Vermiculite", "Unknown"): ("Vermiculite", "Unknown"),
    ("Wollastonite", "Unknown"): ("Wollastonite", "Unknown"),
    ("Zinc", "Crude & refined"): ("Zinc", "Crude & refined"),
    ("Zinc", "Ores & concentrates"): ("Zinc", "Ores & concentrates"),
    ("Zinc", "Oxides"): ("Zinc", "Oxides"),
    ("Zinc", "Scrap"): ("Zinc", "Scrap"),
    ("Zinc", "Unwrought"): ("Zinc", "Unwrought"),
    ("Zinc", "Unwrought alloys"): ("Zinc", "Unwrought alloys"),
    ("Zinc, mine", "Unknown"): ("Zinc", "Mine production"),
    ("Zinc, slab", "Unknown"): ("Zinc", "Slab"),
    ("Zirconium", "Concentrates"): ("Zirconium", "Concentrates"),
    ("Zirconium", "Metal"): ("Zirconium", "Metal"),
    ("Zirconium", "Unknown"): ("Zirconium", "Unknown"),
    ("Zirconium", "Zirconium sand"): ("Zirconium", "Zirconium sand"),
    ("Zirconium minerals", "Unknown"): ("Zirconium minerals", "Unknown"),
}

# Footnotes (that will appear in the footer of charts) to add to the flattened output table.
FOOTNOTES = {
    # Example:
    # 'production|Tungsten|Powder|tonnes': "Tungsten includes...",
}

# There are many historical regions with overlapping data with their successor countries.
# Accept only overlaps on the year when the historical country stopped existing.
ACCEPTED_OVERLAPS = [
    {1991: {"USSR", "Armenia"}},
    {1991: {"USSR", "Belarus"}},
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

    # Visually inspect category-commodity-subcommodity combinations with missing units.
    # Check that the only combinations still with no units are the expected ones.
    missing_units = {
        "category": ["Exports", "Imports"],
        "commodity": ["gemstones", "gemstones"],
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
    tb.loc[(tb["commodity"] == "diamond"), "unit"] = "Carats"
    tb.loc[(tb["commodity"] == "diamond") & (tb["note"].str.lower().str.contains("pounds")), "value"] = None

    # Mapping from original unit names to tonnes.
    mapping = {
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

    # Sanity check.
    error = "Unexpected units found. Add them to the unit mapping and decide its conversion."
    assert set(units) == set(mapping), error

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
            assert set(tb[mask]["commodity"]) == {"helium", "natural gas"}, error
            tb.loc[mask & (tb["commodity"] == "helium"), "value"] *= MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES
            tb.loc[mask & (tb["commodity"] == "natural gas"), "value"] *= MILLION_CUBIC_METERS_OF_NATURAL_GAS_TO_TONNES
        tb.loc[mask, "unit"] = mapping[unit]

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

    # Harmonize units.
    tb = harmonize_units(tb=tb)

    # Improve the name of the commodities.
    tb["commodity"] = tb["commodity"].str.capitalize()

    # Harmonize commodity-subcommodity pairs.
    tb = harmonize_commodity_subcommodity_pairs(tb=tb)

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
    # So, for now, simply add descriptions from producer.
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
