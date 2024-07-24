"""Load, process snapshots, and harmonize.

All these things are done in a single script because the processes are intertwined.

"""
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Tuple
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd

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
# Set to None any commodity-subcommodity that should not be included.
commodity_mapping = {
    ("Aluminum", "Smelter production, aluminum"): ("Aluminum", "Smelter production"),
    ("Aluminum", "Smelter yearend capacity"): None,
    ("Aluminum", "smelter production"): ("Aluminum", "Smelter production"),
    ("Antimony", "Mine production"): ("Antimony", "Mine production"),
    ("Antimony", "Mine production, contained antimony"): ("Antimony", "Mine production"),
    # TODO: In notes, mention that Arsenic - Plant production refers to arsenic trioxide or calculated equivalent.
    ("Arsenic", "Plant production, arsenic trioxide or calculated equivalent."): ("Arsenic", "Plant production"),
    ("Arsenic", "arsenic trioxide"): ("Arsenic", "Plant production"),
    ("Asbestos", "Mine production"): ("Asbestos", "Mine production"),
    ("Asbestos", "Mine production, asbestos"): ("Asbestos", "Mine production"),
    ("Barite", "Mine production"): ("Barite", "Mine production"),
    ("Barite", "Mine production, barite"): ("Barite", "Mine production"),
    ("Barite", "Mine production, excluding U.S."): ("Barite", "Mine production"),
    # TODO: Ensure that the units say calcined equivalent weights, or otherwise add it to the notes.
    ("Bauxite and alumina", "Alumina, refinery production - calcined equivalent weights"): (
        "Alumina",
        "Refinery production",
    ),
    ("Bauxite and alumina", "Bauxite, mine production"): ("Bauxite", "Mine production"),
    ("Bauxite and alumina", "Mine production, bauxite"): ("Bauxite", "Mine production"),
    # TODO: Ensure that the units say dry tons, otherwise add it to the notes.
    ("Bauxite and alumina", "Mine production, bauxite, dry tons"): ("Bauxite", "Mine production"),
    # TODO: Ensure that the units say calcined equivalent weights, or otherwise add it to the notes.
    ("Bauxite and alumina", "Refinery production, alumina - calcined equivalent weights"): (
        "Alumina",
        "Refinery production",
    ),
    ("Beryllium", "Mine production"): ("Beryllium", "Mine production"),
    ("Beryllium", "Mine production, beryllium"): ("Beryllium", "Mine production"),
    ("Bismuth", "Mine production"): ("Bismuth", "Mine production"),
    ("Bismuth", "Refinery production"): ("Bismuth", "Refinery production"),
    ("Boron", "Mine production, borates"): ("Boron", "Mine production, borates"),
    ("Boron", "Mine production, boric oxide equivalent"): ("Boron", "Mine production, boric oxide equivalent"),
    ("Boron", "Mine production, crude borates"): ("Boron", "Mine production, crude borates"),
    ("Boron", "Mine production, crude ore"): ("Boron", "Mine production, crude ore"),
    ("Boron", "Mine production, datolite ore"): ("Boron", "Mine production, datolite ore"),
    ("Boron", "Mine production, ulexite"): ("Boron", "Mine production, ulexite"),
    ("Boron", "Plant production, compounds"): ("Boron", "Plant production, boron compounds"),
    ("Boron", "Plant production, refined borates"): ("Boron", "Plant production, refined borates"),
    ("Boron", "borates"): ("Boron", "Mine production, borates"),
    ("Boron", "boric oxide eqivalent"): ("Boron", "Mine production, boric oxide equivalent"),
    ("Boron", "boric oxide equivalent"): ("Boron", "Mine production, boric oxide equivalent"),
    ("Boron", "boron compounds"): ("Boron", "Plant production, boron compounds"),
    ("Boron", "boron, crude ore"): ("Boron", "Mine production, crude ore"),
    ("Boron", "compounds"): ("Boron", "Plant production, boron compounds"),
    ("Boron", "crude borates"): ("Boron", "Mine production, crude borates"),
    ("Boron", "crude ore"): ("Boron", "Mine production, crude ore"),
    ("Boron", "datolite ore"): ("Boron", "Mine production, datolite ore"),
    ("Boron", "refined borates"): ("Boron", "Plant production, refined borates"),
    ("Boron", "ulexite"): ("Boron", "Mine production, ulexite"),
    ("Bromine", "Plant production, bromine content"): ("Bromine", "Plant production"),
    ("Bromine", "Production, bromine content"): ("Bromine", "Plant production"),
    ("Bromine", "Production"): ("Bromine", "Plant production"),
    ("Cadmium", "Refinery production"): ("Cadmium", "Refinery production"),
    ("Cement", "Cement production, estimated"): ("Cement", "Production"),
    ("Cement", "Clinker capacity, estimated"): None,
    # TODO: Add a note saying that Chromium mine production refers to gross weight (if this is not already mentioned).
    ("Chromium", "Mine production, marketable chromite ore"): ("Chromium", "Mine production, marketable chromite ore"),
    ("Chromium", "Mine production, grosss weight, marketable chromite ore"): (
        "Chromium",
        "Mine production, marketable chromite ore",
    ),
    ("Chromium", "Mne production, marketable chromite ore, gross weight"): (
        "Chromium",
        "Mine production, marketable chromite ore",
    ),
    ("Clays", "Bentonite, mine production"): ("Clays", "Mine production, bentonite"),
    ("Clays", "Fuller's earth, mine production"): ("Clays", "Mine production, fuller's earth"),
    ("Clays", "Kaolin, mine production"): ("Clays", "Mine production, kaolin"),
    ("Clays", "Mine poduction, Bentonite"): ("Clays", "Mine production, bentonite"),
    ("Clays", "Mine poduction, Fuller's earth"): ("Clays", "Mine production, fuller's earth"),
    ("Clays", "Mine poduction, Kaolin"): ("Clays", "Mine production, kaolin"),
    ("Cobalt", "Mine production, contained cobalt"): ("Cobalt", "Mine production"),
    ("Cobalt", "Mine production, metric tons of contained cobalt"): ("Cobalt", "Mine production"),
    ("Copper", "Mine production, contained copper"): ("Copper", "Mine production"),
    # TODO: I'm assuming "Mine production, contained copper" is equivalent to "Mine production, recoverable copper content".
    #  The former is used in the 2022 file, and the latter is used in the 2023 and 2024 files.
    ("Copper", "Mine production, recoverable copper content"): ("Copper", "Mine production"),
    ("Copper", "Refinery production"): ("Copper", "Refinery production"),
    ("Copper", "Refinery production, copper"): ("Copper", "Refinery production"),
    ("Diamond (industrial)", "Mine production"): ("Diamond (industrial)", "Mine production"),
    ("Diamond (industrial)", "Mine production, industrial diamond"): ("Diamond (industrial)", "Mine production"),
    ("Diatomite", "Mine production"): ("Diatomite", "Mine production"),
    ("Diatomite", "Mine production, diatomite"): ("Diatomite", "Mine production"),
    # TODO: In 2022, for Feldspar and nepheline syenite, the subcommodity is "Mine production", whereas in 2023 and 2024, it is "Mine production, feldspar".
    #  Maybe 2022 also refers to only feldspar?
    ("Feldspar and nepheline syenite", "Mine production"): ("Feldspar and nepheline syenite", "Mine production"),
    ("Feldspar and nepheline syenite", "Mine production, feldspar"): ("Feldspar", "Mine production"),
    ("Fluorspar", "Mine production"): ("Fluorspar", "Mine production"),
    ("Fluorspar", "Mine production, fluorspar"): ("Fluorspar", "Mine production"),
    ("Gallium", "Primary production"): ("Gallium", "Primary production"),
    ("Garnet", "Mine production"): ("Garnet", "Mine production"),
    ("Gemstones", "Mine production, thousand carats of gem diamond"): ("Gemstones", "Mine production"),
    ("Gemstones", "Mine production, thousand carats of gem-quality diamond"): ("Gemstones", "Mine production"),
    ("Germanium", "Primary and secondary refinery production"): (
        "Germanium",
        "Primary and secondary refinery production",
    ),
    ("Gold", "Gold - contained content, mine production, metric tons"): ("Gold", "Mine production"),
    ("Gold", "Mine production"): ("Gold", "Mine production"),
    ("Graphite (natural)", "Mine production"): ("Graphite (natural)", "Mine production"),
    ("Graphite (natural)", "Mine production, graphite"): ("Graphite (natural)", "Mine production"),
    ("Gypsum", "Mine production"): ("Gypsum", "Mine production"),
    ("Helium", "Mine production"): ("Helium", "Mine production"),
    ("Helium", "Mine production, helium"): ("Helium", "Mine production"),
    ("Indium", "Refinery production"): ("Indium", "Refinery production"),
    ("Indium", "Refinery production, indium"): ("Indium", "Refinery production"),
    ("Iodine", "Mine production"): ("Iodine", "Mine production"),
    # TODO: Add note saying that Iodine - Mine production refers to elemental iodine.
    ("Iodine", "Mine production, elemental iodine"): ("Iodine", "Mine production"),
    ("Iron and steel", "Pig iron"): ("Iron and steel", "Pig iron"),
    ("Iron and steel", "Pig iron, million metric tons"): ("Iron and steel", "Pig iron"),
    ("Iron and steel", "Raw steel"): ("Iron and steel", "Raw steel"),
    ("Iron and steel", "Raw steel, million metric tons"): ("Iron and steel", "Raw steel"),
    ("Iron ore", "Iron ore - mine production - iron content - thousand metric tons"): ("Iron ore", "Mine production"),
    ("Iron ore", "Iron ore - mine production - usable ore -thousand metric tons"): (
        "Iron ore",
        "Mine production, usable ore",
    ),
    ("Iron ore", "Mine production - Iron content"): ("Iron ore", "Mine production"),
    ("Iron ore", "Mine production - Usable ore"): ("Iron ore", "Mine production, usable ore"),
    ("Iron oxide pigments", "Iron oxide pigments"): ("Iron oxide pigments", "Mine production"),
    ("Iron oxide pigments", "Mine production"): ("Iron oxide pigments", "Mine production"),
    ("Iron oxide pigments", "Mine production, iron oxide pigments"): ("Iron oxide pigments", "Mine production"),
    ("Iron oxide pigments", "Mine production, iron oxide pigments (ocher and red iron oxide)"): (
        "Iron oxide pigments",
        "Mine production, ocher and red iron oxide",
    ),
    ("Iron oxide pigments", "Mine production, iron oxide pigments (ocher)"): (
        "Iron oxide pigments",
        "Mine production, ocher",
    ),
    ("Iron oxide pigments", "Mine production, iron oxide pigments (umber)"): (
        "Iron oxide pigments",
        "Mine production, umber",
    ),
    ("Kyanite", "Kyanite and Related Minerals"): ("Kyanite", "Mine production, kyanite and related minerals"),
    ("Kyanite", "Mine production, andalusite"): ("Kyanite", "Mine production, andalusite"),
    ("Kyanite", "Mine production, kyanite"): ("Kyanite", "Mine production, kyanite"),
    ("Kyanite", "Mine production, kyanite and sillimanite"): ("Kyanite", "Mine production, kyanite and sillimanite"),
    ("Kyanite", "andalusite"): ("Kyanite", "Mine production, andalusite"),
    ("Kyanite", "kyanite"): ("Kyanite", "Mine production, kyanite"),
    ("Kyanite", "kyanite and sillimanite"): ("Kyanite", "Mine production, kyanite and sillimanite"),
    ("Lead", "Mine production"): ("Lead", "Mine production"),
    ("Lead", "Mine production, lead content"): ("Lead", "Mine production"),
    ("Lime", "Plant production"): ("Lime", "Plant production"),
    ("Lime", "Plant production, lime"): ("Lime", "Plant production"),
    ("Lithium", "Mine production, contained lithium"): ("Lithium", "Mine production"),
    ("Lithium", "Mine production, lithium content"): ("Lithium", "Mine production"),
    ("Magnesium compounds", "Mine production"): ("Magnesium compounds", "Mine production"),
    (
        "Magnesium compounds",
        "Mine production, gross weight of magnesite (magnesium carbonate) in thousand metric tons",
    ): ("Magnesium compounds", "Mine production, magnesium carbonate"),
    ("Magnesium compounds", "Mine production, magnesite - contained magnesium oxide (MgO)"): (
        "Magnesium compounds",
        "Mine production, magnesium oxide",
    ),
    ("Magnesium metal", "Magnesium smelter production"): ("Magnesium metal", "Smelter production"),
    ("Magnesium metal", "Smelter production"): ("Magnesium metal", "Smelter production"),
    ("Manganese", "Mine production, manganese content"): ("Manganese", "Mine production"),
    ("Mercury", "Mine production"): ("Mercury", "Mine production"),
    ("Mercury", "Mine production, mercury content"): ("Mercury", "Mine production"),
    ("Mica (natural)", "Mica mine production - scrap and flake"): (
        "Mica (natural)",
        "Mine production, scrap and flake",
    ),
    ("Mica (natural)", "Mica mine production - sheet"): ("Mica (natural)", "Mica mine production, sheet"),
    ("Mica (natural)", "Mine production - mica scrap and flake"): (
        "Mica (natural)",
        "Mine production, scrap and flake",
    ),
    ("Mica (natural)", "Mine production - mica sheet"): ("Mica (natural)", "Mine production, sheet"),
    ("Molybdenum", "Mine production, contained molybdenum"): ("Molybdenum", "Mine production"),
    ("Molybdenum", "Molybdenum mine production, contained molybdenum"): ("Molybdenum", "Mine production"),
    ("Nickel", "Mine production"): ("Nickel", "Mine production"),
    ("Nickel", "Mine production - nickel, metric tons contained"): ("Nickel", "Mine production"),
    ("Niobium (columbium)", "Mine production"): ("Niobium", "Mine production"),
    ("Niobium (columbium)", "Mine production, niobium content"): ("Niobium", "Mine production"),
    ("Nitrogen (fixed)-ammonia", "Plant production"): ("Nitrogen", "Plant production, fixed ammonia"),
    ("Nitrogen (fixed)-ammonia", "Plant production, ammonia - contained nitrogen"): (
        "Nitrogen",
        "Plant production, fixed ammonia",
    ),
    ("Peat", "Mine production"): ("Peat", "Mine production"),
    ("Peat", "Mine production, peat"): ("Peat", "Mine production"),
    ("Perlite", "Mine production"): ("Perlite", "Mine production"),
    ("Perlite", "Mine production, perlite"): ("Perlite", "Mine production"),
    ("Phosphate rock", "Mine production"): ("Phosphate rock", "Mine production"),
    ("Phosphate rock", "Mine production, phosphate rock ore"): ("Phosphate rock", "Mine production"),
    ("Platinum-group metals", "Mine production: Palladium"): ("Palladium", "Mine production"),
    ("Platinum-group metals", "Mine production: Platinum"): ("Platinum", "Mine production"),
    ("Platinum-group metals", "World mine production: Palladium"): ("Palladium", "Mine production"),
    ("Platinum-group metals", "World mine production: Platinum"): ("Platinum", "Mine production"),
    ("Potash", "Mine production"): ("Potash", "Mine production"),
    # TODO: Ensure that the units say potassium oxide equivalent, or otherwise add it to the notes.
    ("Potash", "Mine production, potassium oxide (K2O) equivalent"): ("Potash", "Mine production"),
    ("Pumice and pumicite", "Mine production"): ("Pumice and pumicite", "Mine production"),
    ("Pumice and pumicite", "Mine production, puice and pumicite"): ("Pumice and pumicite", "Mine production"),
    ("Pumice and pumicite", "Mine production, pumice and pumicite"): ("Pumice and pumicite", "Mine production"),
    # TODO: Ensure that the units say rare-earth-oxide equivalent, or otherwise add it to the notes.
    ("Rare earths", "Mine production, metric tons of rare-earth-oxide (REO) equivalent"): (
        "Rare earths",
        "Mine production",
    ),
    ("Rare earths", "Rare earths, mine production, rare-earth-oxide equivalent, metric tons"): (
        "Rare earths",
        "Mine production",
    ),
    ("Rhenium", "Mine production"): ("Rhenium", "Mine production"),
    ("Rhenium", "Mine production, contained rhenium"): ("Rhenium", "Mine production"),
    ("Salt", "Mine production"): ("Salt", "Mine production"),
    ("Sand and gravel (industrial)", "Mine production"): ("Sand and gravel (industrial)", "Mine production"),
    ("Sand and gravel (industrial)", "Mine production, industrial sand and gravel"): (
        "Sand and gravel (industrial)",
        "Mine production",
    ),
    ("Selenium", "Refinery production"): ("Selenium", "Refinery production"),
    ("Selenium", "Refinery production, contained selenium"): ("Selenium", "Refinery production"),
    ("Silicon", "Plant production, silicon content of combined totals for ferrosilicon and silicon metal production"): (
        "Silicon",
        "Plant production, ferrosilicon and silicon metal",
    ),
    ("Silicon", "Plant production, silicon content of ferrosilicon"): ("Silicon", "Plant production, ferrosilicon"),
    ("Silicon", "Plant production, silicon content of ferrosilicon production"): (
        "Silicon",
        "Plant production, ferrosilicon",
    ),
    ("Silicon", "Plant production, silicon metal"): ("Silicon", "Plant production, silicon metal"),
    ("Silicon", "silicon content of combined totals for ferrosilicon and silicon metal production"): (
        "Silicon",
        "Plant production, ferrosilicon and silicon metal",
    ),
    ("Silicon", "silicon content of ferrosilicon production"): ("Silicon", "Plant production, ferrosilicon"),
    ("Silver", "Mine production"): ("Silver", "Mine production"),
    ("Silver", "Silver - mine production, contained silver - metric tons"): ("Silver", "Mine production"),
    ("Silver", "mine production, silver content"): ("Silver", "Mine production"),
    ("Soda ash", "Mine production (natural soda ash)"): ("Soda ash", "Mine production, natural"),
    ("Soda ash", "Soda ash, natural, mine production"): ("Soda ash", "Mine production, natural"),
    ("Soda ash", "Soda ash, synthetic"): ("Soda ash", "Plant production, synthetic"),
    ("Soda ash", "Soda ash, total natural and synthetic"): ("Soda ash", "Total production, natural and synthetic"),
    ("Soda ash", "World total mine production, natural soda ash (rounded)"): ("Soda ash", "Mine production, natural"),
    ("Soda ash", "World total production, natural and synthetic soda ash (rounded)"): (
        "Soda ash",
        "Total production, natural and synthetic",
    ),
    ("Soda ash", "World total production, synthetic soda ash (rounded)"): ("Soda ash", "Plant production, synthetic"),
    ("Stone (dimension)", "Mine production, dimension stone"): ("Stone (dimension)", "Mine production"),
    ("Strontium", "Mine production"): ("Strontium", "Mine production"),
    ("Strontium", "Mine production, contained strontium"): ("Strontium", "Mine production"),
    ("Sulfur", "Production, all forms, contained sulfur"): ("Sulfur", "Production, all forms"),
    ("Sulfur", "Production, all forms, thousand metric tons contained sulfur"): ("Sulfur", "Production, all forms"),
    ("Talc and pyrophyllite", "Mine production, Crude and benficiated talc and pyrophyllite"): (
        "Talc and pyrophyllite",
        "Mine production, crude and beneficiated",
    ),
    ("Talc and pyrophyllite", "Mine production, crude talc"): ("Talc", "Mine production, crude"),
    ("Talc and pyrophyllite", "Mine production, talc"): ("Talc", "Mine production"),
    ("Talc and pyrophyllite", "Mine production, talc (includes steatite)"): (
        "Talc",
        "Mine production, including steatite",
    ),
    ("Talc and pyrophyllite", "Mine production, talc and pyrophyllite"): ("Talc and pyrophyllite", "Mine production"),
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
    ("Tantalum", "Mine production"): ("Tantalum", "Mine production"),
    ("Tantalum", "Mine production, tantalum content"): ("Tantalum", "Mine production"),
    ("Tellurium", "Mine production"): ("Tellurium", "Mine production"),
    ("Tellurium", "Refinery production, tellurium content"): ("Tellurium", "Refinery production"),
    ("Tin", "Mine production, metric tons contained tin"): ("Tin", "Mine production"),
    ("Tin", "Mine production, tin content"): ("Tin", "Mine production"),
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
    # TODO: Ensure that the units say titanium dioxide (TiO2) content, or otherwise add it to the notes.
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
    ): ("Titanium mineral concentrates", "Mine production, ilmenite and rutile"),
    ("Titanium mineral concentrates", "World total mine production: rutile (rounded)"): (
        "Titanium mineral concentrates",
        "Mine production, rutile",
    ),
    (
        "Titanium mineral concentrates",
        "World total mine production: rutile (rounded), titanium dioxide (TiO2) content.",
    ): ("Titanium mineral concentrates", "Mine production, rutile"),
    ("Tungsten", "Mine production, contained tungsten"): ("Tungsten", "Mine production"),
    ("Tungsten", "Mine production, tungsten content"): ("Tungsten", "Mine production"),
    ("Vanadium", "Mine production"): ("Vanadium", "Mine production"),
    ("Vanadium", "Mine production, vanadium content"): ("Vanadium", "Mine production"),
    ("Vermiculite", "Mine production"): ("Vermiculite", "Mine production"),
    ("Wollastonite", "Mine production"): ("Wollastonite", "Mine production"),
    ("Wollastonite", "Mine production, wollastonite"): ("Wollastonite", "Mine production"),
    ("Zeolites (natural)", "Mine production"): ("Zeolites (natural)", "Mine production"),
    ("Zeolites (natural)", "Mine production, zeolites"): ("Zeolites (natural)", "Mine production"),
    ("Zinc", "Mine production, zinc content of concentrates and direct shipping ores"): (
        "Zinc",
        "Mine production, concentrates and direct shipping ores",
    ),
    ("Zirconium and hafnium", "Mine production, zirconium ores and zircon concentrates"): (
        "Zirconium and hafnium",
        "Mine production, zirconium ores and zircon concentrates",
    ),
    (
        "Zirconium and hafnium",
        "Zirconium ores and zircon concentrates, mine production, thousand metric tons, gross weight",
    ): ("Zirconium and hafnium", "Mine production, zirconium ores and zircon concentrates"),
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
        index_issue = d[d["Reserves_kt"] == "Included with “Other countries.”"].index
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
    # Remove spurious commas from numbers, like "7,200,000".
    series = series.str.replace(",", "", regex=False)
    # There is also at least one case (2023 Nickel) of ">100000000". Remove the ">".
    series = series.str.replace(">", "", regex=False)
    # There is also at least one case (2022 Helium) of "<1". Remove the "<".
    series = series.str.replace("<", "", regex=False)
    # There is also at least one case (2022 Vermiculite) of 'Less than ½ unit.'. Replace it by "0.5".
    series = series.str.replace("Less than ½ unit.", "0.5", regex=False)
    # There is also at least one case (2024 Sand and gravel (industrial)) of numbers starting with "e", e.g. "4200".
    # I suppose that probably means "estimated" (and the corresponding notes mention the word "estimated").
    # Remove those "e".
    series = series.str.replace("e", "", regex=False)
    # Convert to float.
    series = series.astype("Float64")

    return series


def _include_note(d: pd.DataFrame, column: str, note: str) -> pd.Series:
    # Include a note in "column" (which is the column devoted for notes).
    d = d.copy()
    if column not in d.columns:
        d[column] = note
    else:
        d[column] = d[column].fillna("")
        d.loc[d[column].str.endswith("."), column] += " " + note
        d.loc[
            (d[column].str.strip().apply(len) > 0) & ~(d[column].str.endswith(".")),
            column,
        ] += ". " + note
        d.loc[d[column] == "", column] = note
    return d[column]


def prepare_reserves_data(d: pd.DataFrame):
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
            d["Reserves_notes"] = _include_note(d=d, column="Reserves_notes", note="Reserves refer to ore.")
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


def prepare_production_data(d: pd.DataFrame):
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
            d["Production_notes"] = _include_note(
                d=d, column="Production_notes", note="Production refers to titanium sponge."
            )
        if d["Mineral"].unique().item() == "Soda ash":  # type: ignore
            # For consistency with different years, rename one of the sub-commodities (this happens at least in 2024).
            d["Type"] = d["Type"].replace({"Soda ash, Synthetic": "Soda ash, synthetic"})

        # Create a Year column and a single column for production.
        df_production = pd.DataFrame()
        for year in years_production:
            columns_to_keep = COMMON_COLUMNS.copy()
            if ("Production_notes" in d.columns) and (year == years_production[-1]):
                # Include the column of production notes only once (for example, when adding data for the last year).
                columns_to_keep += ["Production_notes"]
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
    for pair_old, pair_new in commodity_mapping.items():
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

            # Prepare reserves data.
            _df_reserves = prepare_reserves_data(d=d)

            # For now, ignore capacity data (which appears in a few commodities).
            # columns_capacity = [column for column in d.columns if column.lower().startswith("cap_")]

            # Prepare production data.
            _df_production = prepare_production_data(d=d)

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and gather all their contents.
    data = {}
    for year in YEARS_TO_PROCESS:
        snap = paths.load_snapshot(f"mineral_commodity_summaries_{year}.zip")
        data[year] = extract_data_and_metadata_from_compressed_file(zip_file_path=snap.path)

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
            "Mineral": "commodity",
            "Type": "sub_commodity",
            "Reserves_t": "reserves",
            "Production_t": "production",
        },
        errors="raise",
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "commodity", "sub_commodity"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
