"""Load a snapshot and create a meadow dataset."""

import re
import tempfile
import warnings
from pathlib import Path
from typing import Dict, Tuple
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd
from docx import Document
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Ignore UserWarnings from openpyxl, that repeatedly shows "Unknown extension is not supported and will be removed", even though the loading works well.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

MANUALLY_EXTRACTED_TEXT = {
    "aluminum": """
World Production
Data are defined as world primary aluminum production. Data are reported in the MR and MYB.
""",
    "chromium": """
    World Production
World production is an estimate of world chromite ore mine production measured in contained chromium. World production reported in gross weight was converted to contained chromium by assuming that its chromic oxide content was the same as that of chromite ore imported into the United States. Before content of chromite ore was reported, a time-averaged value was used.
""",
    "vanadium": """
World Production
World production data are for mine production of vanadium. Data are from the MR and MYB for 1912-22, 1925, 1927-31, 1934-43, 1945-47, and 1998 to the most recent year, the CDS for 1960-77, and the MCS for 1978-84 and 1990-97. Data were not available for 1901-11, 1923-24, and 1948-59. World production was interpolated to two significant figures for 1926, 1932-33, 1944, and 1985-89. World production data for 1927-31 and 1997-99 do not contain U.S. production.
""",
    "tellurium": """
World Production
World production data relate to refinery output only. Thus, countries that produced tellurium concentrate or other impure mixtures containing tellurium from copper ores, copper concentrates, blister copper, and/or refinery residues, but did not recover or report refined tellurium, are excluded. The world production table in the MR and MYB is not totaled because of exclusion of data from major world producers, notably the former Soviet Union and the United States. In addition to the countries listed in the world production table (Canada, Japan, Peru, and the United States), Australia, Belgium, Chile, Germany, Kazakhstan, the Philippines, and Russia are known to have produced refined tellurium, but output is not reported; available information is inadequate for formulation of reliable estimates of output levels. World production estimates do not include U.S. production data for 1931 and 1976-2003 because the U.S. data are proprietary. After 2003, total world production was not available. Data are from the MR and the MYB.
""",
    "selenium": """World Production
World production data represent world refinery production of selenium metal. Data were not available for 1900-37. World production estimates for 1985-1987 and 1997 to the most recent year do not include withheld U.S. production data. Data are from the MR and the MYB. Australia, Iran, Kazakhstan, Mexico, the Philippines, and Uzbekistan are known to produce refined selenium, but output is not reported, and information is inadequate for formulation of reliable production estimates. Production increase from 2011 to 2012 is as the result of the inclusion of new country data.""",
    "feldspar": """World Production
World production data for 1908 to the most recent year represent the quantity of feldspar that was produced annually throughout the world as reported in the MR and the MYB. World production data do not include production data for nepheline syenite.""",
    "silver": """World Production
World production data for 1900 to the most recent year represent the recoverable silver content of precious-metal ores that were extracted from mines throughout the world. World production data were from the MR and the MYB.""",
}


def read_data_for_all_commodities(snap: Snapshot) -> Tuple[Dict[str, pr.ExcelFile], Dict[str, str]]:
    # Initialize a dictionary that will gather the excel supply-demand data for each file.
    supply_demand_data = {}
    # Initialize a dictionary that will gather the extracted text from the embedded Word documents.
    extracted_text = {}
    # Open the zip file.
    with ZipFile(snap.path, "r") as zipf:
        # Create a temporary directory.
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Extract supply_demand files.
            supply_demand_dir = temp_path / "supply_demand"
            zipf.extractall(path=temp_path)

            # The metadata is stored in a word document attached to the main excel sheet.
            # Save attached word document in a separate folder.
            word_dir = temp_path / "word_documents"

            # Iterate through the extracted files and apply process_file().
            for excel_path in supply_demand_dir.glob("*.xlsx"):
                supply_demand_data[excel_path.stem] = pr.ExcelFile(excel_path)
                extract_embedded_files_from_excel(excel_path=excel_path, output_dir=word_dir)
                word_file = (word_dir / excel_path.stem).with_suffix(".docx")
                # For some strange reason, chromium text is extracted properly, but the words "United States" disappear!
                # No idea why this is happening; for now, I'll skip extraction and add the text manually.
                if word_file.exists() and (excel_path.stem not in ["chromium"]):
                    # Extract text from the embedded Word document.
                    extracted_text[excel_path.stem] = extract_text_from_word_document(word_file)
                else:
                    if excel_path.stem in MANUALLY_EXTRACTED_TEXT:
                        extracted_text[excel_path.stem] = MANUALLY_EXTRACTED_TEXT[excel_path.stem]
                    else:
                        log.warning(
                            f"Text from '.doc' file needs to be manually extracted for: {excel_path.stem} and added to MANUALLY_EXTRACTED_TEXT."
                        )

    return supply_demand_data, extracted_text


def extract_embedded_files_from_excel(excel_path: Path, output_dir: Path) -> None:
    # Ensure the output directory exists
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # Open the Excel file as a zip archive
    with ZipFile(excel_path, "r") as excel_zip:
        # List all the files in the zip archive
        file_list = excel_zip.namelist()

        # Detect all word documents.
        word_files = [Path(file_name) for file_name in file_list if file_name.endswith((".docx", ".doc"))]

        # Ensure there is only one word document.
        if len(word_files) != 1:
            if excel_path.stem != "antimony":
                log.warning(f"Expected one word document, found {len(word_files)} in: {excel_path}")

        for file_name in word_files:
            # Extract the embedded object.
            embedded_file_path = (output_dir / excel_path.stem).with_suffix(file_name.suffix)
            with open(embedded_file_path, "wb") as embedded_file:
                embedded_file.write(excel_zip.read(file_name.as_posix()))


def extract_text_from_word_document(file_path: Path) -> str:
    # NOTE: This will only work for ".docx" files, not ".doc".
    # Load Word document.
    doc = Document(file_path.as_posix())

    # Extract text from each paragraph
    text = "\n".join([para.text for para in doc.paragraphs])

    return text


def clean_sheet_data(data: pr.ExcelFile, commodity: str, sheet_name: str) -> pd.DataFrame:
    # Extract some useful data and do some basic checks.
    _df = data.parse(sheet_name)
    error = f"Unexpected format in file {commodity}."
    assert _df.iloc[0, 0] == "U.S. GEOLOGICAL SURVEY", error
    assert _df.iloc[1, 0].strip().startswith("[") and _df.iloc[1, 0].strip().endswith("]"), error
    # If the word "gross" appears in the unit, check that it refers to gross weight.
    unit = _df.iloc[1, 0][1:-1]
    assert "metric tons" in unit, error
    if "gross" in unit:
        assert "gross weight" in unit, error
        unit = "metric tonnes of gross weight"
    else:
        unit = "metric tonnes"
    assert _df.iloc[2, 0].lower().startswith("last modification"), error

    # Parse the data properly.
    if _df.iloc[3, 0] == "Year":
        # This is the most common case.
        df = data.parse(sheet_name, skiprows=4)
    else:
        # This happens at least to "Pig iron" sheet of "iron_and_steel".
        df = data.parse(sheet_name, skiprows=5)
    assert df.columns[0] == "Year", error

    # Remove columns with all NaN values.
    df = df.dropna(axis=1, how="all").reset_index(drop=True)

    # Clean spurious spaces from column names (e.g. "Production ").
    df.columns = [re.sub(r"\s+", " ", column).strip() for column in df.columns]

    # Remove spurious "Unnamed: X" columns.
    df = df.drop(columns=[column for column in df.columns if column.startswith("Unnamed")])

    # Add commodity and unit columns.
    df["commodity"] = sheet_name
    df["unit"] = unit

    # Extract notes written below the table in the first column.
    notes = list(df.loc[~df["Year"].astype("string").str.isdigit()]["Year"])
    # One of the notes is always the citation (but the order changes).
    _citation = [note[1:] for note in notes if "1Compiled" in str(note)]
    assert len(_citation) == 1, error
    # Add source as a new column.
    df["source"] = _citation[0]

    # Remove notes from the table.
    df = df.loc[df["Year"].astype("string").str.isdigit()].reset_index(drop=True)

    # Rename some columns so they are consistent with other tables.
    df = df.rename(
        columns={
            "Unit value ($/t)": "Unit value $/t",
            "Unit value (98 $/t)": "Unit value 98$/t",
            "Unit value (98$/t)": "Unit value 98$/t",
            # Note that we inform if the weight is gross in the "unit" column.
            "World production (gross weight)": "World production",
        },
        errors="ignore",
    )

    return df


def combine_data_for_all_commodities(
    supply_demand_data: Dict[str, pr.ExcelFile], supply_demand_metadata: Dict[str, str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Initialize a dataframe that will combine the data for all commodities.
    combined = pd.DataFrame()
    combined_metadata = pd.DataFrame()
    for commodity, data in supply_demand_data.items():
        if commodity == "abrasives__natural__discontinued__see_garnet__industrial":
            # This file has a different format, with two tables in the same sheet.
            # For now, simply skip it.
            continue

        if commodity == "nickel":
            # For commodities with multiple sheets, the sheets correspond to different commodities.
            # For example, "bauxite_and_alumina" has a sheet for "Bauxite" and another for "Alumina".
            # However, for "nickel", the sheets show more detailed nickel data, which for now we don't need.
            sheet_names = ["Nickel"]
        else:
            sheet_names = data.sheet_names

        for sheet_name in sheet_names:
            if sheet_name == "Sheet1":
                # This spurious empty sheet appears sometimes; simply skip it.
                continue
            df = clean_sheet_data(data=data, commodity=commodity, sheet_name=sheet_name)

            # Add the dataframe for the current commodity to the combined dataframe.
            combined = pd.concat([combined, df])

            # Gather metadata for the current commodity (from the text in the word document inside the excel file).
            metadata = supply_demand_metadata[commodity]
            df_metadata = pd.DataFrame({"commodity": [sheet_name]})
            for column in df.columns:
                paragraph = extract_paragraph(text=metadata, header=column)
                if len(paragraph) > 0:
                    df_metadata[column] = paragraph
            combined_metadata = pd.concat([combined_metadata, df_metadata], ignore_index=True)

    # Sanity check.
    assert set([column for column in combined.columns if "value" in column.lower()]) == {
        "Unit value $/t",
        "Unit value 98$/t",
    }

    # For some reason, "Boron" appears twice in the metadata, with identical content. Drop repeated row.
    combined_metadata = combined_metadata.drop_duplicates().reset_index(drop=True)

    return combined, combined_metadata


def extract_paragraph(text: str, header: str) -> str:
    start_paragraph = False
    paragraph_text = ""
    for line in text.split("\n"):
        if line.strip().lower().replace("(", "").replace(")", "").startswith(
            header.lower().replace("(", "").replace(")", "")
        ) and (len(line.strip()) < 1.2 * len(header)):
            start_paragraph = True
            # Skip title, but start storing text from the next line.
            continue

        if start_paragraph:
            paragraph_text += line + "\n"

        if len(line.strip()) == 0:
            # The paragraph has ended.
            start_paragraph = False
    return paragraph_text.strip()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historical_statistics_for_mineral_and_material_commodities.zip")

    # Gather data (as ExcelFile objects) for all commodities, and metadata (extracted from the text of word documents embedded in the excel files).
    supply_demand_data, supply_demand_metadata = read_data_for_all_commodities(snap=snap)

    #
    # Process data.
    #
    # Process and combine data and extracted metadata for all commodities.
    combined, combined_metadata = combine_data_for_all_commodities(
        supply_demand_data=supply_demand_data, supply_demand_metadata=supply_demand_metadata
    )

    # Create a table with metadata.
    tb = pr.read_from_df(data=combined, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True)

    # Columns contain a mix of numbers and strings (I suppose corresponding to footnotes).
    # For now, save all data as strings.
    tb = tb.astype({column: "string" for column in tb.columns})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # NOTE: There are duplicated rows with different data, e.g. Nickel 2019. We'll fix it in the garden step.
    tb = tb.format(["commodity", "year"], verify_integrity=False)

    # Create a table with metadata for the extracted metadata.
    tb_metadata = pr.read_from_df(
        data=combined_metadata, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True
    )
    tb_metadata = tb_metadata.format(["commodity"], short_name=paths.short_name + "_metadata")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb, tb_metadata], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
