"""Load a snapshot and create a meadow dataset."""
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd

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
# Convert million cubic meters of helium to metric tonnes.
MILLION_CUBIC_METERS_OF_HELIUM_TO_TONNES = 178.5


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
            note = "Reserves refer to ore."
            if "Reserves_notes" not in d.columns:
                d["Reserves_notes"] = note
            else:
                d["Reserves_notes"] = d["Reserves_notes"].fillna("")
                d.loc[d["Reserves_notes"].str.endswith("."), "Reserves_notes"] += " " + note
                d.loc[
                    (d["Reserves_notes"].str.strip().apply(len) > 0) & ~(d["Reserves_notes"].str.endswith(".")),
                    "Reserves_notes",
                ] += ". " + note
                d.loc[d["Reserves_notes"] == "", "Reserves_notes"] = note
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

        # Create a Year column.
        columns_to_keep = COMMON_COLUMNS
        if "Prod_notes" in d.columns:
            columns_to_keep += ["Prod_notes"]
        df_production = pd.DataFrame()
        for year in years_production:
            _column_production = [column for column in columns_production if str(year) in column][0]
            _df_for_year = (
                d[columns_to_keep + [_column_production]]
                .rename(columns={_column_production: "Production_t"}, errors="raise")
                .assign(**{"Year": year})
            )
            df_production = pd.concat([df_production, _df_for_year], ignore_index=True)

        # Fix units.
        if unit_production not in ["kt", "t", "kg"]:
            # TODO: Handle special cases.
            # mcm (Hellium), kct (Gemstones), Mt (Iron and steel), mmt (Iron and steel), mct (Diamond (industrial)), Sponge_t (Titanium and titanium dioxide).
            return None

        if unit_production == "kt":
            df_production["Production_t"] *= 1e3

        # Remove rows without data.
        df_production = df_production.dropna(subset=["Production_t"], how="all").reset_index(drop=True)

        # TODO: Production notes are missing. Fix it.

        return df_production


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

    # TODO: Consider if using the original metadata from xml files.

    # Create a table with metadata.
    tb = pr.read_from_df(df, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)  # type: ignore

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "mineral", "type"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
