"""Load, process snapshots, and harmonize.

All these things are done in a single script because the processes are intertwined.

"""
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table, VariablePresentationMeta
from tqdm.auto import tqdm

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List years for which snapshots are available.
YEARS_TO_PROCESS = [2022, 2023, 2024]

# List all spurious values that appear in the data (after being stripped of empty spaces) and should be replaced by nan.
NA_VALUES = ["", "NA", "XX", "Large", 'Categorized as "large"', "Very small", "W", "—"]

# Define common columns.
# NOTE: Column "Mineral" will be added in processing. It corresponds to the name of the commodity (and it will often be similar to "Type", but less specific).
COMMON_COLUMNS = ["Source", "Country", "Mineral", "Type"]

# Convert million carats to metric tonnes.
MILLION_CARATS_TO_TONNES = 0.2
# Convert thousand carats to metric tonnes.
THOUSAND_CARATS_TO_TONNES = 0.2e-3
# Convert million cubic meters of helium to metric tonnes.
MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES = 178.5


# Harmonize commodity-subcommodity names.
# NOTE: This list should contain all commodity-subcommodity pairs expected in the data.
#  Set to None any commodity-subcommodity that should not be included.
# NOTE: Sometimes the original names include relevant information.
#  This information will be included in the footnotes (see FOOTNOTES defined below).
# NOTE: An easy way to bulk-replace entities is by searching and replacing with regex in VSCode.
#  For example, to replace "Mine production" with "Mine" in all values, search for ': (\("[^"]+", ")(Mine production)(")' and replace with ': $1Mine$3'.
COMMODITY_MAPPING = {
    ("Aluminum", "Smelter production, aluminum"): ("Aluminum", "Smelter"),
    ("Aluminum", "Smelter yearend capacity"): None,
    ("Aluminum", "smelter production"): ("Aluminum", "Smelter"),
    ("Antimony", "Mine production"): ("Antimony", "Mine"),
    ("Antimony", "Mine production, contained antimony"): ("Antimony", "Mine"),
    ("Arsenic", "Plant production, arsenic trioxide or calculated equivalent."): ("Arsenic", "Processing"),
    ("Arsenic", "arsenic trioxide"): ("Arsenic", "Processing"),
    ("Asbestos", "Mine production"): ("Asbestos", "Mine"),
    ("Asbestos", "Mine production, asbestos"): ("Asbestos", "Mine"),
    ("Barite", "Mine production"): ("Barite", "Mine"),
    ("Barite", "Mine production, barite"): ("Barite", "Mine"),
    ("Barite", "Mine production, excluding U.S."): ("Barite", "Mine"),
    ("Bauxite and alumina", "Alumina, refinery production - calcined equivalent weights"): (
        "Alumina",
        "Refinery",
    ),
    ("Bauxite and alumina", "Bauxite, mine production"): ("Bauxite", "Mine"),
    ("Bauxite and alumina", "Mine production, bauxite"): ("Bauxite", "Mine"),
    ("Bauxite and alumina", "Mine production, bauxite, dry tons"): ("Bauxite", "Mine"),
    ("Bauxite and alumina", "Refinery production, alumina - calcined equivalent weights"): (
        "Alumina",
        "Refinery",
    ),
    ("Beryllium", "Mine production"): ("Beryllium", "Mine"),
    ("Beryllium", "Mine production, beryllium"): ("Beryllium", "Mine"),
    ("Bismuth", "Mine production"): ("Bismuth", "Mine"),
    ("Bismuth", "Refinery production"): ("Bismuth", "Refinery"),
    # NOTE: None of the following seem to correspond to the Boron mine production from the USGS historical data.
    ("Boron", "Mine production, borates"): None,
    ("Boron", "Mine production, boric oxide equivalent"): None,
    ("Boron", "Mine production, crude borates"): None,
    ("Boron", "Mine production, crude ore"): None,
    ("Boron", "Mine production, datolite ore"): None,
    ("Boron", "Mine production, ulexite"): None,
    ("Boron", "Plant production, compounds"): None,
    ("Boron", "Plant production, refined borates"): None,
    ("Boron", "borates"): None,
    ("Boron", "boric oxide eqivalent"): None,
    ("Boron", "boric oxide equivalent"): None,
    ("Boron", "boron compounds"): None,
    ("Boron", "boron, crude ore"): None,
    ("Boron", "compounds"): None,
    ("Boron", "crude borates"): None,
    ("Boron", "crude ore"): None,
    ("Boron", "datolite ore"): None,
    ("Boron", "refined borates"): None,
    ("Boron", "ulexite"): None,
    ("Bromine", "Plant production, bromine content"): ("Bromine", "Processing"),
    ("Bromine", "Production, bromine content"): ("Bromine", "Processing"),
    ("Bromine", "Production, bromine content, excluding U.S. production"): ("Bromine", "Processing"),
    ("Bromine", "Production"): ("Bromine", "Processing"),
    ("Cadmium", "Refinery production"): ("Cadmium", "Refinery"),
    ("Cement", "Cement production, estimated"): ("Cement", "Total"),
    ("Cement", "Clinker capacity, estimated"): None,
    ("Chromium", "Mine production, marketable chromite ore"): ("Chromium", "Mine"),
    ("Chromium", "Mne production, grosss weight, marketable chromite ore"): ("Chromium", "Mine"),
    ("Chromium", "Mne production, marketable chromite ore, gross weight"): ("Chromium", "Mine"),
    ("Clays", "Bentonite, mine production"): ("Clays", "Bentonite"),
    ("Clays", "Fuller's earth, mine production"): ("Clays", "Fuller's earth"),
    ("Clays", "Kaolin, mine production"): ("Clays", "Kaolin"),
    ("Clays", "Mine poduction, Bentonite"): ("Clays", "Bentonite"),
    ("Clays", "Mine poduction, Fuller's earth"): ("Clays", "Fuller's earth"),
    ("Clays", "Mine poduction, Kaolin"): ("Clays", "Kaolin"),
    ("Cobalt", "Mine production, contained cobalt"): ("Cobalt", "Mine"),
    ("Cobalt", "Mine production, metric tons of contained cobalt"): ("Cobalt", "Mine"),
    ("Copper", "Mine production, contained copper"): ("Copper", "Mine"),
    # NOTE: I'm assuming "Mine production, contained copper" is equivalent to "Mine production, recoverable copper content".
    #  The former is used in the 2022 file, and the latter is used in the 2023 and 2024 files.
    ("Copper", "Mine production, recoverable copper content"): ("Copper", "Mine"),
    ("Copper", "Refinery production"): ("Copper", "Refinery"),
    ("Copper", "Refinery production, copper"): ("Copper", "Refinery"),
    ("Diamond (industrial)", "Mine production"): ("Diamond", "Mine, industrial"),
    ("Diamond (industrial)", "Mine production, industrial diamond"): ("Diamond", "Mine, industrial"),
    ("Diatomite", "Mine production"): ("Diatomite", "Mine"),
    ("Diatomite", "Mine production, diatomite"): ("Diatomite", "Mine"),
    # NOTE: In 2022, for Feldspar and nepheline syenite, the subcommodity is "Mine production", whereas in 2023 and 2024, it is "Mine production, feldspar".
    #  Maybe 2022 also refers to only feldspar?
    ("Feldspar and nepheline syenite", "Mine production"): ("Feldspar and nepheline syenite", "Mine"),
    ("Feldspar and nepheline syenite", "Mine production, feldspar"): ("Feldspar", "Mine"),
    ("Fluorspar", "Mine production"): ("Fluorspar", "Mine"),
    ("Fluorspar", "Mine production, fluorspar"): ("Fluorspar", "Mine"),
    ("Gallium", "Primary production"): ("Gallium", "Primary"),
    ("Garnet", "Mine production"): ("Garnet", "Mine"),
    ("Gemstones", "Mine production, thousand carats of gem diamond"): ("Gemstones", "Mine"),
    ("Gemstones", "Mine production, thousand carats of gem-quality diamond"): ("Gemstones", "Mine"),
    ("Germanium", "Primary and secondary refinery production"): (
        "Germanium",
        "Primary and secondary refinery",
    ),
    ("Gold", "Gold - contained content, mine production, metric tons"): ("Gold", "Mine"),
    ("Gold", "Mine production"): ("Gold", "Mine"),
    ("Graphite (natural)", "Mine production"): ("Graphite", "Mine"),
    ("Graphite (natural)", "Mine production, graphite"): ("Graphite", "Mine"),
    ("Gypsum", "Mine production"): ("Gypsum", "Mine"),
    ("Helium", "Mine production"): ("Helium", "Mine"),
    ("Helium", "Mine production, helium"): ("Helium", "Mine"),
    ("Indium", "Refinery production"): ("Indium", "Refinery"),
    ("Indium", "Refinery production, indium"): ("Indium", "Refinery"),
    ("Iodine", "Mine production"): ("Iodine", "Mine"),
    ("Iodine", "Mine production, elemental iodine"): ("Iodine", "Mine"),
    ("Iron and steel", "Pig iron"): ("Iron", "Pig iron"),
    ("Iron and steel", "Pig iron, million metric tons"): ("Iron", "Pig iron"),
    ("Iron and steel", "Raw steel"): ("Steel", "Crude"),
    ("Iron and steel", "Raw steel, million metric tons"): ("Steel", "Crude"),
    ("Iron ore", "Iron ore - mine production - iron content - thousand metric tons"): ("Iron ore", "Iron content"),
    # Looking at the metadata notes, "usable ore" corresponds to "Crude ore".
    ("Iron ore", "Iron ore - mine production - usable ore -thousand metric tons"): ("Iron ore", "Crude ore"),
    ("Iron ore", "Mine production - Iron content"): ("Iron ore", "Iron content"),
    ("Iron ore", "Mine production - Usable ore"): ("Iron ore", "Crude ore"),
    ("Iron oxide pigments", "Iron oxide pigments"): None,
    ("Iron oxide pigments", "Mine production"): None,
    ("Iron oxide pigments", "Mine production, iron oxide pigments"): None,
    ("Iron oxide pigments", "Mine production, iron oxide pigments (ocher and red iron oxide)"): None,
    ("Iron oxide pigments", "Mine production, iron oxide pigments (ocher)"): None,
    ("Iron oxide pigments", "Mine production, iron oxide pigments (umber)"): None,
    ("Kyanite", "Kyanite and Related Minerals"): ("Kyanite", "Mine, kyanite and related minerals"),
    ("Kyanite", "Mine production, andalusite"): ("Kyanite", "Mine, andalusite"),
    ("Kyanite", "Mine production, kyanite"): ("Kyanite", "Mine, kyanite"),
    ("Kyanite", "Mine production, kyanite and sillimanite"): ("Kyanite", "Mine, kyanite and sillimanite"),
    ("Kyanite", "andalusite"): ("Kyanite", "Mine, andalusite"),
    ("Kyanite", "kyanite"): ("Kyanite", "Mine, kyanite"),
    ("Kyanite", "kyanite and sillimanite"): ("Kyanite", "Mine, kyanite and sillimanite"),
    ("Lead", "Mine production"): ("Lead", "Mine"),
    ("Lead", "Mine production, lead content"): ("Lead", "Mine"),
    ("Lime", "Plant production"): ("Lime", "Processing"),
    ("Lime", "Plant production, lime"): ("Lime", "Processing"),
    ("Lithium", "Mine production, contained lithium"): ("Lithium", "Mine"),
    ("Lithium", "Mine production, lithium content"): ("Lithium", "Mine"),
    ("Magnesium compounds", "Mine production"): ("Magnesium compounds", "Mine"),
    (
        "Magnesium compounds",
        "Mine production, gross weight of magnesite (magnesium carbonate) in thousand metric tons",
    ): ("Magnesium compounds", "Mine, magnesium carbonate"),
    ("Magnesium compounds", "Mine production, magnesite - contained magnesium oxide (MgO)"): (
        "Magnesium compounds",
        "Mine, magnesium oxide",
    ),
    ("Magnesium metal", "Magnesium smelter production"): ("Magnesium metal", "Smelter"),
    ("Magnesium metal", "Smelter production"): ("Magnesium metal", "Smelter"),
    ("Manganese", "Mine production, manganese content"): ("Manganese", "Mine"),
    ("Mercury", "Mine production"): ("Mercury", "Mine"),
    ("Mercury", "Mine production, mercury content"): ("Mercury", "Mine"),
    ("Mica (natural)", "Mica mine production - scrap and flake"): (
        "Mica (natural)",
        "Mine, scrap and flake",
    ),
    ("Mica (natural)", "Mica mine production - sheet"): ("Mica (natural)", "Mine, sheet"),
    ("Mica (natural)", "Mine production - mica scrap and flake"): (
        "Mica (natural)",
        "Mine, scrap and flake",
    ),
    ("Mica (natural)", "Mine production - mica sheet"): ("Mica (natural)", "Mine, sheet"),
    ("Molybdenum", "Mine production, contained molybdenum"): ("Molybdenum", "Mine"),
    ("Molybdenum", "Molybdenum mine production, contained molybdenum"): ("Molybdenum", "Mine"),
    ("Nickel", "Mine production"): ("Nickel", "Mine"),
    ("Nickel", "Mine production - nickel, metric tons contained"): ("Nickel", "Mine"),
    ("Niobium (columbium)", "Mine production"): ("Niobium", "Mine"),
    ("Niobium (columbium)", "Mine production, niobium content"): ("Niobium", "Mine"),
    ("Nitrogen (fixed)-ammonia", "Plant production"): ("Nitrogen", "Plant, fixed ammonia"),
    ("Nitrogen (fixed)-ammonia", "Plant production, ammonia - contained nitrogen"): (
        "Nitrogen",
        "Plant, fixed ammonia",
    ),
    ("Peat", "Mine production"): ("Peat", "Mine"),
    ("Peat", "Mine production, peat"): ("Peat", "Mine"),
    ("Perlite", "Mine production"): ("Perlite", "Mine"),
    ("Perlite", "Mine production, perlite"): ("Perlite", "Mine"),
    ("Phosphate rock", "Mine production"): ("Phosphate rock", "Mine"),
    ("Phosphate rock", "Mine production, phosphate rock ore"): ("Phosphate rock", "Mine"),
    ("Platinum-group metals", "Mine production: Palladium"): ("Palladium", "Mine"),
    ("Platinum-group metals", "Mine production: Platinum"): ("Platinum", "Mine"),
    ("Platinum-group metals", "World mine production: Palladium"): ("Palladium", "Mine"),
    ("Platinum-group metals", "World mine production: Platinum"): ("Platinum", "Mine"),
    ("Potash", "Mine production"): ("Potash", "Mine"),
    ("Potash", "Mine production, potassium oxide (K2O) equivalent"): ("Potash", "Mine"),
    ("Pumice and pumicite", "Mine production"): ("Pumice and pumicite", "Mine"),
    ("Pumice and pumicite", "Mine production, puice and pumicite"): ("Pumice and pumicite", "Mine"),
    ("Pumice and pumicite", "Mine production, pumice and pumicite"): ("Pumice and pumicite", "Mine"),
    ("Rare earths", "Mine production, metric tons of rare-earth-oxide (REO) equivalent"): (
        "Rare earths",
        "Mine",
    ),
    ("Rare earths", "Rare earths, mine production, rare-earth-oxide equivalent, metric tons"): (
        "Rare earths",
        "Mine",
    ),
    ("Rhenium", "Mine production"): ("Rhenium", "Mine"),
    ("Rhenium", "Mine production, contained rhenium"): ("Rhenium", "Mine"),
    ("Salt", "Mine production"): ("Salt", "Mine"),
    ("Sand and gravel (industrial)", "Mine production"): (
        "Sand and gravel",
        "Industrial",
    ),
    ("Sand and gravel (industrial)", "Mine production, industrial sand and gravel"): (
        "Sand and gravel",
        "Industrial",
    ),
    ("Selenium", "Refinery production"): ("Selenium", "Refinery"),
    ("Selenium", "Refinery production, contained selenium"): ("Selenium", "Refinery"),
    ("Silicon", "Plant production, silicon content of combined totals for ferrosilicon and silicon metal production"): (
        "Silicon",
        "Processing",
    ),
    ("Silicon", "Plant production, silicon content of ferrosilicon"): None,
    ("Silicon", "Plant production, silicon content of ferrosilicon production"): None,
    ("Silicon", "Plant production, silicon metal"): None,
    ("Silicon", "silicon content of combined totals for ferrosilicon and silicon metal production"): (
        "Silicon",
        "Processing",
    ),
    ("Silicon", "silicon content of ferrosilicon production"): None,
    ("Silver", "Mine production"): ("Silver", "Mine"),
    ("Silver", "Silver - mine production, contained silver - metric tons"): ("Silver", "Mine"),
    ("Silver", "mine production, silver content"): ("Silver", "Mine"),
    ("Soda ash", "Mine production (natural soda ash)"): ("Soda ash", "Mine, natural"),
    ("Soda ash", "Soda ash, natural, mine production"): ("Soda ash", "Mine, natural"),
    ("Soda ash", "Soda ash, synthetic"): ("Soda ash", "Plant production, synthetic"),
    ("Soda ash", "Soda ash, total natural and synthetic"): ("Soda ash", "Total production, natural and synthetic"),
    ("Soda ash", "World total mine production, natural soda ash (rounded)"): ("Soda ash", "Mine, natural"),
    ("Soda ash", "World total production, natural and synthetic soda ash (rounded)"): (
        "Soda ash",
        "Total production, natural and synthetic",
    ),
    ("Soda ash", "World total production, synthetic soda ash (rounded)"): ("Soda ash", "Plant production, synthetic"),
    ("Stone (dimension)", "Mine production, dimension stone"): ("Dimension stone", "Mine"),
    ("Strontium", "Mine production"): ("Strontium", "Mine"),
    ("Strontium", "Mine production, contained strontium"): ("Strontium", "Mine"),
    ("Sulfur", "Production, all forms, contained sulfur"): ("Sulfur", "Production, all forms"),
    ("Sulfur", "Production, all forms, thousand metric tons contained sulfur"): ("Sulfur", "Production, all forms"),
    ("Talc and pyrophyllite", "Mine production, Crude and benficiated talc and pyrophyllite"): (
        "Talc and pyrophyllite",
        "Mine production, crude and beneficiated",
    ),
    ("Talc and pyrophyllite", "Mine production, crude talc"): ("Talc", "Mine, crude"),
    ("Talc and pyrophyllite", "Mine production, talc"): ("Talc", "Mine"),
    ("Talc and pyrophyllite", "Mine production, talc (includes steatite)"): (
        "Talc",
        "Mine production, including steatite",
    ),
    ("Talc and pyrophyllite", "Mine production, talc and pyrophyllite"): ("Talc and pyrophyllite", "Mine"),
    ("Talc and pyrophyllite", "Mine production, talc and pyrophyllite (includes crude)"): (
        "Talc and pyrophyllite",
        "Mine production, including crude",
    ),
    ("Talc and pyrophyllite", "Mine production, talc and pyrophyllite (rounded)"): (
        "Talc and pyrophyllite",
        "Mine production",
    ),
    ("Talc and pyrophyllite", "Mine production, unspecified talc and/or pyrophyllite"): (
        "Talc and pyrophyllite",
        "Mine production, unspecified talc and/or pyrophyllite",
    ),
    ("Tantalum", "Mine production"): ("Tantalum", "Mine"),
    ("Tantalum", "Mine production, tantalum content"): ("Tantalum", "Mine"),
    ("Tellurium", "Mine production"): ("Tellurium", "Mine"),
    ("Tellurium", "Refinery production, tellurium content"): ("Tellurium", "Refinery"),
    ("Tin", "Mine production, metric tons contained tin"): ("Tin", "Mine"),
    ("Tin", "Mine production, tin content"): ("Tin", "Mine"),
    ("Titanium and titanium dioxide", "Sponge Metal Production and Sponge and Pigment Capacity"): (
        "Titanium and titanium dioxide",
        "Sponge Metal Production and Sponge and Pigment Capacity",
    ),
    ("Titanium and titanium dioxide", "Sponge Metal Production and Sponge and Pigment Yearend Operating Capacity"): (
        "Titanium and titanium dioxide",
        "Sponge Metal Production and Sponge and Pigment Yearend Operating Capacity",
    ),
    ("Titanium mineral concentrates", "Mine production: Ilmenite"): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite",
    ),
    ("Titanium mineral concentrates", "Mine production: Ilmenite (rounded)"): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite",
    ),
    ("Titanium mineral concentrates", "Mine production: Ilmenite and rutile"): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite and rutile",
    ),
    ("Titanium mineral concentrates", "Mine production: Ilmenite, titanium dioxide (TiO2) content."): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite",
    ),
    ("Titanium mineral concentrates", "Mine production: rutile"): (
        "Titanium mineral concentrates",
        "Mine production, rutile",
    ),
    ("Titanium mineral concentrates", "Mine production: rutile, titanium dioxide (TiO2) content."): (
        "Titanium mineral concentrates",
        "Mine production, rutile",
    ),
    ("Titanium mineral concentrates", "World total mine production: Ilmenite (rounded)"): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite",
    ),
    ("Titanium mineral concentrates", "World total mine production: Ilmenite, titanium dioxide (TiO2) content."): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite",
    ),
    ("Titanium mineral concentrates", "World total mine production: ilmentite and rutile (rounded)"): (
        "Titanium mineral concentrates",
        "Mine production, ilmenite and rutile",
    ),
    (
        "Titanium mineral concentrates",
        "World total mine production: ilmentite and rutile (rounded), titanium dioxide (TiO2) content.",
    ): ("Titanium mineral concentrates", "Mine, ilmenite and rutile"),
    ("Titanium mineral concentrates", "World total mine production: rutile (rounded)"): (
        "Titanium mineral concentrates",
        "Mine production, rutile",
    ),
    (
        "Titanium mineral concentrates",
        "World total mine production: rutile (rounded), titanium dioxide (TiO2) content.",
    ): ("Titanium mineral concentrates", "Mine, rutile"),
    ("Tungsten", "Mine production, contained tungsten"): ("Tungsten", "Mine"),
    ("Tungsten", "Mine production, tungsten content"): ("Tungsten", "Mine"),
    ("Vanadium", "Mine production"): ("Vanadium", "Mine"),
    ("Vanadium", "Mine production, vanadium content"): ("Vanadium", "Mine"),
    ("Vermiculite", "Mine production"): ("Vermiculite", "Mine"),
    ("Wollastonite", "Mine production"): ("Wollastonite", "Mine"),
    ("Wollastonite", "Mine production, wollastonite"): ("Wollastonite", "Mine"),
    ("Zeolites (natural)", "Mine production"): ("Zeolites (natural)", "Mine"),
    ("Zeolites (natural)", "Mine production, zeolites"): ("Zeolites (natural)", "Mine"),
    ("Zinc", "Mine production, zinc content of concentrates and direct shipping ores"): (
        "Zinc",
        "Mine",
    ),
    ("Zirconium and hafnium", "Mine production, zirconium ores and zircon concentrates"): (
        "Zirconium and hafnium",
        "Mine production, zirconium ores and zircon concentrates",
    ),
    (
        "Zirconium and hafnium",
        "Zirconium ores and zircon concentrates, mine production, thousand metric tons, gross weight",
    ): ("Zirconium and hafnium", "Mine, zirconium ores and zircon concentrates"),
}

# Footnotes (that will appear in the footer of charts) to add to the flattened output table.
FOOTNOTES = {
    "production|Arsenic|Processing|tonnes": "Values are reported as arsenic trioxide or calculated equivalent.",
    "production|Iodine|Mine|tonnes": "Values refer to elemental iodine.",
    "reserves|Iodine|Mine|tonnes": "Values refer to elemental iodine.",
    "production|Graphite|Mine|tonnes": "Values refer to natural graphite.",
    "reserves|Graphite|Mine|tonnes": "Values refer to natural graphite.",
    "production|Silicon|Processing|tonnes": "Values refer to silicon content of ferrosilicon and silicon metal.",
    "reserves|Bauxite|Mine|tonnes": "Values are reported as dried bauxite equivalents.",
}

# Dictionary of special units.
UNITS_MAPPING = {
    # "production|Arsenic|Processing|tonnes": "tonnes of arsenic trioxide or calculated equivalent",
    # "production|Alumina|Refinery|tonnes": "tonnes of calcined equivalent weights",
    # "production|Bauxite|Mine|tonnes": "tonnes of dry content",
    # "reserves|Bauxite|Mine|tonnes": "tonnes of dry content",
    # "production|Chromium|Mine|tonnes": "tonnes of gross weight",
    "reserves|Chromium|Mine|tonnes": "tonnes of gross weight",
    "production|Potash|Mine|tonnes": "tonnes of potassium oxide equivalent",
    "reserves|Potash|Mine|tonnes": "tonnes of potassium oxide equivalent",
    "production|Rare earths|Mine|tonnes": "tonnes of rare-earth-oxide equivalent",
    "reserves|Rare earths|Mine|tonnes": "tonnes of rare-earth-oxide equivalent",
    "production|Titanium mineral concentrates|Mine production, ilmenite|tonnes": "tonnes of titanium dioxide content",
    "reserves|Titanium mineral concentrates|Mine production, ilmenite|tonnes": "tonnes of titanium dioxide content",
    "production|Titanium mineral concentrates|Mine production, rutile|tonnes": "tonnes of titanium dioxide content",
    "reserves|Titanium mineral concentrates|Mine production, rutile|tonnes": "tonnes of titanium dioxide content",
    "production|Titanium mineral concentrates|Mine production, ilmenite and rutile|tonnes": "tonnes of titanium dioxide content",
    "reserves|Titanium mineral concentrates|Mine production, ilmenite and rutile|tonnes": "tonnes of titanium dioxide content",
}


def extract_metadata_from_xml(file_path):
    # Remove spurious symbols from the XML file (this happens at least to 2024 mcs2024-plati_meta.xml file).
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read().replace("&", "&amp;")

    root = ET.fromstring(content)

    # Initialize an empty dictionary to store the extracted data.
    world_data = {}

    # Extract the title (which contains the commodity name).
    title_element = root.find(".//title")
    if title_element is not None:
        title_text = title_element.text
        # Extract commodity name from the title.
        if " - " in title_text:  # type: ignore
            commodity_name = title_text.split(" - ")[1].replace(" Data Release", "").strip()  # type: ignore
            world_data["Commodity"] = commodity_name

    # Navigate to the world data section.
    for detailed in root.findall(".//detailed"):
        enttypl = detailed.find("enttyp/enttypl")
        if enttypl is not None and "world" in enttypl.text.lower():  # type: ignore
            for attr in detailed.findall("attr"):
                attr_label = attr.find("attrlabl")
                attr_def = attr.find("attrdef")
                if attr_label is not None and attr_def is not None:
                    world_data[attr_label.text] = attr_def.text.strip()  # type: ignore

    return world_data


def extract_data_and_metadata_from_compressed_file(zip_file_path: Path):
    data_for_year = {}
    # Open the zip file.
    with ZipFile(zip_file_path, "r") as zipf:
        # Create a temporary directory.
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            # Extract data files.
            zipf.extractall(path=temp_path)

            # Iterate through the extracted files and apply process_file().
            for file_path in sorted(temp_path.glob("**/*.csv")):
                _metadata_path = file_path.with_suffix(".xml").as_posix().replace("_world", "_meta")
                encoding = "utf-8"
                ####################################################################################################
                # Handle special cases.
                if file_path.stem == "mcs2023-zirco-hafni_world":
                    _metadata_path = file_path.with_name("mcs2023-zirco_meta.xml")
                elif file_path.stem == "mcs2022-garne_world":
                    encoding = "latin1"
                elif file_path.stem == "MCS2024-bismu_world":
                    # This patch is not necessary in case-insensitive file systems, but I'll add it just in case.
                    _metadata_path = file_path.with_name("mcs2024-bismu_meta.xml")
                ####################################################################################################
                # Read data and metadata.
                _data = pd.read_csv(file_path, encoding=encoding)
                _metadata = extract_metadata_from_xml(_metadata_path)

                # Store data and metadata in the common dictionary.
                data_for_year[_metadata["Commodity"]] = {"data": _data, "metadata": _metadata}

    return data_for_year


def extract_and_clean_data_for_year_and_mineral(data: Dict[int, Any], year: int, mineral: str) -> pd.DataFrame:
    d = data[year][mineral]["data"].copy()
    # Remove empty rows and columns.
    d = d.dropna(axis=1, how="all")
    d = d.dropna(how="all").reset_index(drop=True)

    ############################################################################################################
    # Handle special cases.
    if (year == 2022) & (mineral == "CADMIUM"):
        # Instead of "Type", there is a "Form" column.
        d = d.rename(columns={"Form": "Type"}, errors="raise")
    if (year == 2024) & (mineral == "IRON OXIDE PIGMENTS"):
        # Column "Type" is simply missing, and there is no replacement.
        # Create a column with the mineral name instead.
        d["Type"] = mineral.capitalize()
    if (year == 2024) & (mineral == "FLUORSPAR"):
        d = d.rename(columns={"Mine production, fluorspar": "Type"}, errors="raise")
    if mineral == "ABRASIVES (MANUFACTURED)":
        # There is no "Source" column.
        d["Source"] = f"MCS{year}"
    if (year == 2024) & (
        mineral in ["ALUMINUM", "IRON OXIDE PIGMENTS", "GARNET", "ZEOLITES (NATURAL)", "IRON OXIDE PIGMENTS"]
    ):
        # Same (minor) issue: missing source column, or source is spelled differently (namely "MCS 2024").
        # Fix it for consistency (so other assertions don't fail).
        d["Source"] = f"MCS{year}"
    if (year == 2024) & (mineral == "BARITE"):
        # There is an extra column "Unnamed: 8" that has data, which seems to be the same as "Reserves_kt".
        d = d.drop(columns=["Unnamed: 8"])
    if (year == 2024) & (mineral == "VERMICULITE"):
        # There is a "Reserves_kt" column with 7 missing values, and then a "Reserves_notes" with no notes, but,
        # coincidentally or not, 7 numerical values. Then, there is a "Unnamed: 8" with notes about reserves.
        # So, it seems that these columns were not properly constructed, so I'll remove them.
        d = d.drop(columns=["Reserves_kt", "Reserves_notes", "Unnamed: 8"])
    if (year == 2024) & (mineral == "ZIRCONIUM AND HAFNIUM"):
        # The reserves column usually does not include a year, but in this case it does.
        # For consistency, remove it from the column name.
        d = d.rename(columns={"Reserves_kt_2023": "Reserves_kt"}, errors="raise")
    if mineral == "POTASH":
        # Reserves are given as "Reserves_ore_kt" and "Reserves_ore_K2O_equiv_kt".
        # For consistency with other minerals, keep only the former.
        # NOTE: This happens for 2023 and 2024, not 2022.
        d = d.drop(columns=["Reserves_ore_K2O_equiv_kt"], errors="ignore")
    if (year == 2023) & (mineral == "PEAT"):
        # There are spurious notes in "Reserves_kt", and no column for "Reserves_notes".
        # Fix this.
        index_issue = d.loc[d["Reserves_kt"] == "Included with “Other countries.”"].index
        d.loc[index_issue, "Reserves_kt"] = pd.NA
        assert "Reserves_notes" not in d.columns
        d["Reserves_notes"] = pd.NA
        d.loc[index_issue, "Reserves_notes"] = "Included with “Other countries.”"
    if (year in [2023, 2024]) & (mineral == "GRAPHITE (NATURAL)"):
        # Column "Reserves_t" contains rows that say 'Included in World total.' (and again, it has no notes).
        # Make them nan and include them in notes.
        index_issue = d[d["Reserves_t"] == "Included in World total."].index
        d.loc[index_issue, "Reserves_t"] = pd.NA
        assert "Reserves_notes" not in d.columns
        d["Reserves_notes"] = pd.NA
        d.loc[index_issue, "Reserves_notes"] = "Included in World total."
    if (year in [2023, 2024]) & (mineral == "TITANIUM MINERAL CONCENTRATES"):
        # One row contains "Included with ilmenite" (and 'included with ilmenite') in all data columns (but for
        # whatever reason not in any of the "notes" columns).
        # For simplicity, simply remove this row.
        d = d[d["Reserves_kt"].str.lower() != "included with ilmenite"].reset_index(drop=True)

    ############################################################################################################

    # Check that all columns are either source, country, type, production, reserves, or capacity.
    assert all(
        [column.lower().startswith(("source", "country", "type", "prod_", "reserves_", "cap_")) for column in d.columns]
    )
    # Sanity checks.
    assert d["Source"].unique().item() == f"MCS{year}"
    assert "Country" in d.columns
    assert "Type" in d.columns

    # Sometimes there is a "Prod_notes" and sometimes a "Prod_note" column.
    # For consistency, rename the latter to "Production_notes".
    d = d.rename(columns={"Prod_note": "Production_notes", "Prod_notes": "Production_notes"}, errors="ignore")

    # Add a column for the mineral name (although it will usually be very similar to "Type").
    d["Mineral"] = mineral.capitalize()

    # Remove spurious spaces.
    d["Type"] = d["Type"].str.strip().str.replace("  ", " ", regex=False)

    return d


def clean_spurious_values(series: pd.Series) -> pd.Series:
    series = series.copy()
    # Remove spurious spaces like "   NA".
    series = series.fillna("").astype(str).str.strip()
    for na_value in NA_VALUES:
        series = series.replace(na_value, pd.NA)
    series = (
        # Remove spurious commas from numbers, like "7,200,000".
        series.str.replace(",", "", regex=False)
        # There is also at least one case (2023 Nickel) of ">100000000". Remove the ">".
        .str.replace(">", "", regex=False)
        # There is also at least one case (2022 Helium) of "<1". Remove the "<".
        .str.replace("<", "", regex=False)
        # There is also at least one case (2022 Vermiculite) of 'Less than ½ unit.'. Replace it by "0.5".
        .str.replace("Less than ½ unit.", "0.5", regex=False)
        # There is also at least one case (2024 Sand and gravel (industrial)) of numbers starting with "e", e.g. "4200".
        # I suppose that probably means "estimated" (and the corresponding notes mention the word "estimated").
        # Remove those "e".
        .str.replace("e", "", regex=False)
    )

    # Convert to float.
    series = series.astype("Float64")

    return series


def prepare_reserves_data(d: pd.DataFrame, metadata: Dict[str, str]) -> Optional[pd.DataFrame]:
    d = d.copy()
    # Select columns related to reserves data.
    columns_reserves = [
        column for column in d.columns if column.lower().startswith("reserves_") and column != "Reserves_notes"
    ]
    if not any(columns_reserves):
        return None
    else:
        _column_reserves = [column for column in columns_reserves if column != "Reserves_notes"]
        # There is usually either zero or one column for reserves (and usually 2 for production).
        # I have not found any case with more than one column for reserves.
        assert len(_column_reserves) == 1
        column_reserves = _column_reserves[0]
        unit_reserves = "_".join(column_reserves.split("_")[1:])

        # Get general notes for this column, extracted from the xml metadata.
        notes_reserves = metadata.get(column_reserves)
        if notes_reserves is not None:
            d["Reserves_notes"] = [note + [notes_reserves] for note in d["Reserves_notes"]]

        # Clean reserves data.
        d[column_reserves] = clean_spurious_values(series=d[column_reserves])

        # Fix units.
        assert unit_reserves in ["kt", "t", "mct", "Mt", "mt", "mcm", "kg", "ore_kt"]
        if unit_reserves == "mct":
            d["Reserves_mct"] *= MILLION_CARATS_TO_TONNES
            d = d.rename(columns={"Reserves_mct": "Reserves_t"}, errors="raise")
        elif unit_reserves == "kt":
            d["Reserves_kt"] *= 1e3
            d = d.rename(columns={"Reserves_kt": "Reserves_t"}, errors="raise")
        elif unit_reserves == "ore_kt":
            d["Reserves_ore_kt"] *= 1e3
            d = d.rename(columns={"Reserves_ore_kt": "Reserves_t"}, errors="raise")
            # Add a note explaining that the data is for ore.
            d["Reserves_notes"] = [note + ["Reserves refer to ore."] for note in d["Reserves_notes"]]
        elif unit_reserves in ["Mt", "mt"]:
            d[f"Reserves_{unit_reserves}"] *= 1e6
            d = d.rename(columns={f"Reserves_{unit_reserves}": "Reserves_t"}, errors="raise")
        elif (unit_reserves == "mcm") & (d["Mineral"].unique().item() == "Helium"):  # type: ignore
            d["Reserves_mcm"] *= MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES
            d = d.rename(columns={"Reserves_mcm": "Reserves_t"}, errors="raise")
        elif unit_reserves == "kg":
            d["Reserves_kg"] *= 1e-3
            d = d.rename(columns={"Reserves_kg": "Reserves_t"}, errors="raise")

        # Define the columns for the dataframe of reserves data for the current year and mineral.
        columns = COMMON_COLUMNS + ["Reserves_t"]
        if "Reserves_notes" in d.columns:
            columns += ["Reserves_notes"]

        # While production is usually given for an explicit year (actually, usually two years), reserves
        # does not have a year. I will assume that the reserves correspond to the latest informed year.
        year_reserves = int(d["Source"].unique().item()[-4:]) - 1  # type: ignore
        df_reserves = d[columns].assign(**{"Year": year_reserves})

        # Remove rows without data.
        df_reserves = df_reserves.dropna(subset=["Reserves_t"], how="all").reset_index(drop=True)

        return df_reserves


def prepare_production_data(d: pd.DataFrame, metadata: Dict[str, str]) -> Optional[pd.DataFrame]:
    d = d.copy()

    # Select columns related to production data.
    columns_production = [
        column for column in d.columns if column.lower().startswith("prod_") and column != "Production_notes"
    ]
    if not any(columns_production):
        # This happens to "ABRASIVES (MANUFACTURED)".
        return None
    else:
        # There are always two columns for production, corresponding to the last two informed years.
        assert len(columns_production) == 2

        # Clean production data.
        for column in columns_production:
            d[column] = clean_spurious_values(series=d[column])

        # The year can be extracted from the last character of the column name.
        years_production = [int(column[-4:]) for column in columns_production]
        # Adapt metadata dictionary (which has a note for each production column) to have one note for each year.
        metadata_production = {year: metadata.get(columns_production[i]) for i, year in enumerate(years_production)}

        # Sanity check.
        # NOTE: If data changes in a future update, the following can be relaxed.
        assert all(
            [
                (str(year).isdigit()) and (year < max(YEARS_TO_PROCESS)) and (year >= min(YEARS_TO_PROCESS) - 2)
                for year in years_production
            ]
        )

        # Extract the unit.
        # NOTE: Often (possibly always) one of the columns has "_est_" (or "_Est_"), I suppose to signal that it is estimated data.
        _units_production = sorted(
            set(
                [
                    column[:-4].replace("Prod_", "").replace("_est_", "").replace("_Est_", "").rstrip("_")
                    for column in columns_production
                ]
            )
        )
        assert len(_units_production) == 1
        unit_production = _units_production[0]

        # Handle special case.
        if unit_production == "Sponge_t":
            unit_production = "t"
            # Add a note explaining that the data is for ore.
            d["Production_notes"] = [note + ["Production refers to titanium sponge."] for note in d["Reserves_notes"]]
        if d["Mineral"].unique().item() == "Soda ash":  # type: ignore
            # For consistency with different years, rename one of the sub-commodities (this happens at least in 2024).
            d["Type"] = d["Type"].replace({"Soda ash, Synthetic": "Soda ash, synthetic"})

        # Create a Year column and a single column for production.
        df_production = pd.DataFrame()
        for year in years_production:
            columns_to_keep = COMMON_COLUMNS.copy() + ["Production_notes"]

            # Get general notes for this column, extracted from the xml metadata.
            notes_production = metadata_production.get(year)
            if notes_production is not None:
                d["Production_notes"] = [note + [notes_production] for note in d["Production_notes"]]

            _column_production = [column for column in columns_production if str(year) in column][0]
            _df_for_year = (
                d[columns_to_keep + [_column_production]]
                .rename(columns={_column_production: "Production_t"}, errors="raise")
                .assign(**{"Year": year})
            )
            df_production = pd.concat([df_production, _df_for_year], ignore_index=True)

        # Fix units.
        assert unit_production in ["t", "kg", "kt", "Mt", "mmt", "mcm", "kct", "mct"]
        if unit_production == "kg":
            df_production["Production_t"] *= 1e-3
        elif unit_production == "kt":
            df_production["Production_t"] *= 1e3
        elif unit_production in ["Mt", "mmt"]:
            df_production["Production_t"] *= 1e6
        elif (unit_production == "mcm") and (d["Mineral"].unique().item() == "Helium"):  # type: ignore
            df_production["Production_t"] *= MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES
        elif unit_production == "kct":
            df_production["Production_t"] *= THOUSAND_CARATS_TO_TONNES
        elif unit_production == "mct":
            df_production["Production_t"] *= MILLION_CARATS_TO_TONNES

        # Remove rows without data.
        df_production = df_production.dropna(subset=["Production_t"], how="all").reset_index(drop=True)

        return df_production


def harmonize(df):
    # Many country names come with an annotation. For example, specifying a subtype of commodity, or a nuance.
    # Ideally, they could have used a separate column for this, like the one dedicated to notes.
    # But unfortunately, that is not the case.
    #
    # Sometimes, the additional information in the country name is irrelevant.
    # For example, when they mention that the values are rounded.
    #
    # And sometimes, the additional info is also mentioned in the "type" column.
    # For example,
    # "World total (rutile, rounded)" has type "World total mine production: rutile (rounded)..."; and then
    # "World total (ilmenite and rutile, rounded)" has type "World total mine production: ilmentite and rutile (rounded)...".
    #
    # But unfortunately, sometimes the info in the country name is nowhere else.
    # For example, "Japan (quicklime only)"; this nuance does not even appear in the metadata xml file.
    #
    # Luckily, it does not happen often that, for the same country-year-mineral-type, there are multiple rows for the same country (with different annotations).
    # This does happen, however, at least in some cases.
    # For example, for Helium, there is "United States (extracted from natural gas)", and
    # "United States (from Cliffside Field)", both with type "Mine production".
    #
    # To make things worse, countries are often spelled differently (and there is often a spurious "e" at the end).
    # For example, in the files for Perlite 2022 and 2023, there is "Argentina", but for the same commodity in the 2024 file, there is "Argentinae".
    #
    # So, given that we need to remove duplicates in production data (because each year appears in two consecutive data files),
    # we need to harmonize before dropping duplicates.
    #

    # Many countries have a number at the end (e.g., "United States1").
    # Remove all digits from country names.
    df["Country"] = df["Country"].str.replace(r"\d+", "", regex=True)
    # Often, "(rounded)" appears. Remove it.
    df["Country"] = df["Country"].str.replace(r"\(rounded\)", "", regex=True)
    # Often, "\xa0" appears. Remove it.
    df["Country"] = df["Country"].str.replace(r"\xa0", "", regex=True)
    # Remove spurious spaces.
    df["Country"] = df["Country"].str.strip()

    # Harmonize country names.
    # NOTE: For some reason, sometimes "World" excludes the US.
    # For now, we excluded those aggregates from the data.
    # If needed, we can construct proper aggregates in the future.
    df = geo.harmonize_countries(
        df=df,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        country_col="Country",
        warn_on_unused_countries=False,
    )

    return df


def fix_helium_issue(df_reserves: pd.DataFrame, df_production: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # After harmonization, some countries that appeared with an annotation become the same.
    # However, there is only one case where this creates ambiguity within the same country-year-mineral-type.
    # That's Helium. Check that this is the case, and remove this commodity.
    counts = df_reserves.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False).count()
    assert set(counts[counts["Reserves_t"] > 1]["Mineral"]) == set(["Helium"])
    counts = df_production.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False).count()
    assert set(counts[counts["Production_t"] > 2]["Mineral"]) == set(["Helium"])

    # As a simple solution, sum the values of the repeated instances.
    index_issue = df_reserves[(df_reserves["Mineral"] == "Helium") & (df_reserves["Country"] == "United States")].index
    _df_reserves = (
        df_reserves.loc[index_issue]
        .groupby(["Source", "Year", "Country", "Mineral", "Type"], as_index=False)
        .agg({"Reserves_t": "sum", "Source": "last", "Reserves_notes": "last"})
    )
    df_reserves = pd.concat([df_reserves.drop(index_issue), _df_reserves], ignore_index=True)

    # Idem for production.
    index_issue = df_production[
        (df_production["Mineral"] == "Helium") & (df_production["Country"] == "United States")
    ].index
    _df_production = (
        df_production.loc[index_issue]
        .groupby(["Source", "Year", "Country", "Mineral", "Type"], as_index=False)
        .agg({"Production_t": "sum", "Source": "last", "Production_notes": "last"})
    )
    df_production = pd.concat([df_production.drop(index_issue), _df_production], ignore_index=True)

    return df_reserves, df_production


def harmonize_commodity_subcommodity_pairs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    missing_mappings = set([tuple(pair) for pair in df[["Mineral", "Type"]].drop_duplicates().values.tolist()]) - set(
        COMMODITY_MAPPING
    )
    assert len(missing_mappings) == 0, f"Missing mappings: {missing_mappings}"
    # NOTE: Do not assert that all mappings are used, since mappings are shared for reserves and production.
    # unused_mappings = set(COMMODITY_MAPPING) - set([tuple(pair) for pair in df[["Mineral", "Type"]].drop_duplicates().values.tolist()])
    # assert len(unused_mappings) == 0, f"Unused mappings: {unused_mappings}"
    for pair_old, pair_new in COMMODITY_MAPPING.items():
        if pair_old == pair_new:
            # Nothing to do, continue.
            continue

        # Get the old commodity-subcommodity names.
        commodity_old, subcommodity_old = pair_old
        if pair_new is None:
            # Remove rows for this combination.
            index_to_drop = df.loc[(df["Mineral"] == commodity_old) & (df["Type"] == subcommodity_old)].index
            df = df.drop(index_to_drop).reset_index(drop=True)
            continue

        # Get the new commodity-subcommodity names.
        commodity_new, subcommodity_new = pair_new
        # Rename the commodity-subcommodity pair.
        df.loc[(df["Mineral"] == commodity_old) & (df["Type"] == subcommodity_old), ["Mineral", "Type"]] = pair_new

    return df


def gather_and_process_data(data) -> pd.DataFrame:
    # Initialize empty dataframes that will gather all data for reserves and production.
    df_reserves = pd.DataFrame()
    df_production = pd.DataFrame()

    # Go year by year and mineral by mineral, handle special cases, homogenize units, and combine data.
    for year in YEARS_TO_PROCESS:
        for mineral in data[year]:
            # Clean data.
            d = extract_and_clean_data_for_year_and_mineral(data=data, year=year, mineral=mineral)

            # Gather metadata for the current year-mineral.
            metadata = data[year][mineral]["metadata"]

            # Combine general notes with country-year-specific notes.
            if "Production_notes" not in d.columns:
                d["Production_notes"] = None
            d["Production_notes"] = [[note] if pd.notnull(note) else [] for note in d["Production_notes"]]
            if "Reserves_notes" not in d.columns:
                d["Reserves_notes"] = None
            d["Reserves_notes"] = [[note] if pd.notnull(note) else [] for note in d["Reserves_notes"]]

            # Prepare reserves data.
            _df_reserves = prepare_reserves_data(d=d, metadata=metadata)

            # For now, ignore capacity data (which appears in a few commodities).
            # columns_capacity = [column for column in d.columns if column.lower().startswith("cap_")]

            # Prepare production data.
            _df_production = prepare_production_data(d=d, metadata=metadata)

            # Append the new data to the main dataframe.
            if _df_reserves is not None:
                df_reserves = pd.concat([df_reserves, _df_reserves], ignore_index=True)
            if _df_production is not None:
                df_production = pd.concat([df_production, _df_production], ignore_index=True)

    # Harmonize commodity-subcommodity pairs.
    df_reserves = harmonize_commodity_subcommodity_pairs(df=df_reserves)
    df_production = harmonize_commodity_subcommodity_pairs(df=df_production)

    # Check that, before harmonization, there is only 1 instance for each country-year-mineral-type for reserves,
    # and 2 for production. The latter happens because each year is given in two consecutive data files (the first time,
    # as an estimate).
    assert (
        df_reserves.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False)
        .count()["Reserves_t"]
        .max()
        == 1
    )
    assert (
        df_production.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False)
        .count()["Production_t"]
        .max()
        == 2
    )

    # Harmonize country names.
    df_reserves = harmonize(df_reserves)
    df_production = harmonize(df_production)

    # Fix Helium issue (see function for an explanation).
    df_reserves, df_production = fix_helium_issue(df_reserves=df_reserves, df_production=df_production)

    # Check that the issue is solved.
    assert (
        df_reserves.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False)
        .count()["Reserves_t"]
        .max()
        == 1
    )
    assert (
        df_production.groupby(["Country", "Year", "Mineral", "Type"], observed=True, as_index=False)
        .count()["Production_t"]
        .max()
        == 2
    )

    # For each year, there is production data for two years.
    # So, there are multiple values of production data for the same year.
    # Assume that the latest is the most accurate (usually, the previous value was an estimate).
    df_production = df_production.sort_values(
        ["Source", "Country", "Mineral", "Type", "Year"], ascending=True
    ).reset_index(drop=True)
    df_production = df_production.drop_duplicates(
        subset=["Country", "Mineral", "Type", "Year"], keep="last"
    ).reset_index(drop=True)

    # Combine reserves and production data.
    # NOTE: Here, do not merge on "Source". It can happen that we have reserves for one year in one source, but not in the other.
    #  Merging on source would therefore leave spurious nans in the data.
    #  Therefore, remove the "Source" columns.
    df = df_reserves.drop(columns=["Source"]).merge(
        df_production.drop(columns=["Source"]), on=["Country", "Mineral", "Type"] + ["Year"], how="outer"
    )

    return df


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
    for column in tqdm(tb_flat_notes.drop(columns=["country", "year"]).columns, disable=True):
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
    # Retrieve snapshots and gather all their contents.
    data = {}
    for year in YEARS_TO_PROCESS:
        snap = paths.load_snapshot(f"mineral_commodity_summaries_{year}.zip")
        data[year] = extract_data_and_metadata_from_compressed_file(zip_file_path=snap.path)

    # Load regions dataset.
    # ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    # ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Gather and process all reserves and production data.
    df = gather_and_process_data(data=data)

    # Create a table with metadata.
    tb = pr.read_from_df(df, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)  # type: ignore

    # For convenience (and for consistency with other similar datasets) rename columns.
    tb = tb.rename(
        columns={
            "Country": "country",
            "Year": "year",
            "Mineral": "commodity",
            "Type": "sub_commodity",
            "Reserves_t": "reserves",
            "Production_t": "production",
            "Reserves_notes": "notes_reserves",
            "Production_notes": "notes_production",
        },
        errors="raise",
    )

    # Assume that all data is in tonnes.
    # NOTE: Values have already been converted based on column names (e.g. columns often contain "_t", or "_kt").
    #  And later, special cases are handled after flattening the table.
    tb["unit"] = "tonnes"
    tb["unit"] = tb["unit"].copy_metadata(tb["production"])

    # Before creating aggregates, ensure notes are lists of strings.
    for column in ["notes_reserves", "notes_production"]:
        tb[column] = [note if isinstance(note, list) else [] for note in tb[column]]

    # Add regions to the table.
    # NOTE: After inspection, it seems that USGS region aggregates often are significantly lower han BGS regions
    #  aggregates (at least for those indicators where both series overlap). This indicates that USGS' regions may not
    #  be representative enough. Therefore, it seems safer to not build region aggregates for USGS.
    #  For more details, see garden minerals step.
    # tb = geo.add_regions_to_table(
    #     tb=tb,
    #     ds_regions=ds_regions,
    #     ds_income_groups=ds_income_groups,
    #     min_num_values_per_year=1,
    #     index_columns=["country", "year", "commodity", "sub_commodity", "unit"],
    #     countries_that_must_have_data={"North America": ["United States"], "Asia": ["China"]},
    #     # accepted_overlaps=ACCEPTED_OVERLAPS,
    # )

    # Clean notes columns (e.g. remove repeated notes).
    for column in ["notes_reserves", "notes_production"]:
        tb[column] = [clean_notes(note) for note in tb[column]]

    # Gather all notes in a dictionary.
    notes = gather_notes(tb, notes_columns=["notes_production", "notes_reserves"])

    # Create a flattened table and remove empty columns.
    tb_flat = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["production", "reserves"],
        join_column_levels_with="|",
    ).dropna(axis=1, how="all")

    # Handle special units.
    tb["unit_production"] = tb["unit"].copy()
    tb["unit_reserves"] = tb["unit"].copy()
    for column, unit in UNITS_MAPPING.items():
        tb_flat = tb_flat.rename(columns={column: column.replace("|tonnes", f"|{unit}")}, errors="raise")
        # Handle special units for long table too.
        category, commodity, sub_commodity, unit_old = column.split("|")
        tb.loc[(tb["commodity"] == commodity) & (tb["sub_commodity"] == sub_commodity), f"unit_{category}"] = unit

    # NOTE: Here, I could loop over columns and improve metadata.
    # However, for convenience (since this step is not used separately), this will be done in the garden minerals step.
    # So, for now, simply add titles and descriptions from producer.
    for column in tb_flat.drop(columns=["country", "year"]).columns:
        # Create metadata title (before they become snake-case).
        tb_flat[column].metadata.title = column
        if column in notes:
            tb_flat[column].metadata.description_from_producer = "Notes found in original USGS data:\n" + notes[column]

    # To avoid ETL failing when storing the table, convert lists of notes to strings (and add metadata).
    for column in ["notes_reserves", "notes_production"]:
        tb[column] = tb[column].copy_metadata(tb["production"]).astype(str)

    # Add footnotes.
    for column, note in FOOTNOTES.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    # Drop empty columns, if any.
    tb_flat = tb_flat.dropna(axis=1, how="all").reset_index(drop=True)

    # Format tables conveniently.
    tb = tb.format(["country", "year", "commodity", "sub_commodity"], short_name=paths.short_name)
    tb_flat = tb_flat.format(["country", "year"], short_name=paths.short_name + "_flat")
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns})

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_flat], check_variables_metadata=True)
    ds_garden.save()
