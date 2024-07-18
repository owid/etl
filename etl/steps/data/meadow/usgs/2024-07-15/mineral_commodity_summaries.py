"""Load a snapshot and create a meadow dataset."""
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List years for which snapshots are available.
YEARS_TO_PROCESS = [2022, 2023, 2024]

# List all spurious values that appear in the data (after being stripped of empty spaces) and should be replaced by nan.
NA_VALUES = ["", "NA", "XX", "Large", 'Categorized as "large"', "Very small"]


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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and gather all their contents.
    data = {}
    for year in YEARS_TO_PROCESS:
        snap = paths.load_snapshot(f"mineral_commodity_summaries_{year}.zip")
        data[year] = extract_data_and_metadata_from_compressed_file(zip_file_path=snap.path)

    # TODO: Move up.
    # Convert million carats to metric tonnes.
    MILLION_CARATS_TO_TONNES = 0.2
    # Convert million cubic meters of helium to metric tonnes.
    MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES = 178.5
    #
    # Process data.
    #
    for year in YEARS_TO_PROCESS:
        for mineral in data[year]:
            d = data[year][mineral]["data"]
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
                [
                    column.lower().startswith(("source", "country", "type", "prod_", "reserves_", "cap_"))
                    for column in d.columns
                ]
            )

            # Add a column for the mineral name.
            d["Mineral"] = mineral.capitalize()

            columns_production = [column for column in d.columns if column.lower().startswith("prod_")]
            columns_capacity = [column for column in d.columns if column.lower().startswith("cap_")]
            columns_reserves = [
                column for column in d.columns if column.lower().startswith("reserves_") and column != "Reserves_notes"
            ]
            if any(columns_reserves):
                _column_reserves = [column for column in columns_reserves if column != "Reserves_notes"]
                assert len(_column_reserves) == 1
                column_reserves = _column_reserves[0]
                unit_reserves = "_".join(column_reserves.split("_")[1:])

                # Clean reserves data.
                # Remove spurious spaces like "   NA".
                d[column_reserves] = d[column_reserves].fillna("").astype(str).str.strip()
                for na_value in NA_VALUES:
                    d[column_reserves] = d[column_reserves].replace(na_value, pd.NA)
                # Remove spurious commas from numbers, like "7,200,000".
                d[column_reserves] = d[column_reserves].str.replace(",", "", regex=False)
                # There is also at least one case (2023 Nickel) of ">100000000". Remove the ">".
                d[column_reserves] = d[column_reserves].str.replace(">", "", regex=False)
                # Convert to float.
                d[column_reserves] = d[column_reserves].astype("Float64")

                # Fix units.
                assert unit_reserves in ["kt", "t", "mct", "Mt", "mt", "mcm", "kg", "ore_kt"]
                if unit_reserves == "mct":
                    d["Reserves_mct"] = MILLION_CARATS_TO_TONNES * d["Reserves_mct"].astype("Float64")
                    d = d.rename(columns={"Reserves_mct": "Reserves_t"}, errors="raise")
                elif unit_reserves == "kt":
                    d["Reserves_kt"] = 1e3 * d["Reserves_kt"].astype("Float64")
                    d = d.rename(columns={"Reserves_kt": "Reserves_t"}, errors="raise")
                elif unit_reserves in ["Mt", "mt"]:
                    d[f"Reserves_{unit_reserves}"] = 1e6 * d[f"Reserves_{unit_reserves}"].astype("Float64")
                    d = d.rename(columns={f"Reserves_{unit_reserves}": "Reserves_t"}, errors="raise")
                elif (unit_reserves == "mcm") & (mineral == "HELIUM"):
                    d["Reserves_mcm"] = MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES * d["Reserves_mcm"].astype("Float64")
                    d = d.rename(columns={"Reserves_mcm": "Reserves_t"}, errors="raise")
                elif unit_reserves == "kg":
                    d["Reserves_kg"] = 1e-3 * d["Reserves_kg"].astype("Float64")
                    d = d.rename(columns={"Reserves_kg": "Reserves_t"}, errors="raise")
            # Sanity checks.
            assert d["Source"].unique().item() == f"MCS{year}"
            assert "Country" in d.columns
            assert "Type" in d.columns

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
