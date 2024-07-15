"""Load a snapshot and create a meadow dataset."""

import re
import tempfile
import warnings
from pathlib import Path
from zipfile import ZipFile

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Ignore UserWarnings from openpyxl, that repeatedly shows "Unknown extension is not supported and will be removed", even though the loading works well.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


def read_data_for_all_commodities(snap: Snapshot):
    # Initialize a dictionary that will gather the excel supply-demand data for each file.
    supply_demand_data = {}
    # Open the zip file.
    with ZipFile(snap.path, "r") as zipf:
        # Create a temporary directory.
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Extract supply_demand files.
            supply_demand_dir = temp_path / "supply_demand"
            zipf.extractall(path=temp_path)

            # Iterate through the extracted files and apply process_file().
            for file_path in supply_demand_dir.glob("*.xlsx"):
                supply_demand_data[file_path.stem] = pr.ExcelFile(file_path)

    return supply_demand_data


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
    notes = list(df[~df["Year"].astype(str).str.isdigit()]["Year"])
    # One of the notes is always the citation (but the order changes).
    _citation = [note[1:] for note in notes if "1Compiled" in str(note)]
    assert len(_citation) == 1, error
    # Add source as a new column.
    df["source"] = _citation[0]

    # Remove notes from the table.
    df = df[df["Year"].astype(str).str.isdigit()].reset_index(drop=True)

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


def combine_data_for_all_commodities(supply_demand_data: dict) -> pd.DataFrame:
    # Initialize a dataframe that will combine the data for all commodities.
    combined = pd.DataFrame()
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

    # Sanity check.
    assert set([column for column in combined.columns if "value" in column.lower()]) == {
        "Unit value $/t",
        "Unit value 98$/t",
    }
    return combined


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("historical_statistics_for_mineral_and_material_commodities.zip")

    # Gather data (as ExcelFile objects) for all commodities.
    supply_demand_data = read_data_for_all_commodities(snap=snap)

    #
    # Process data.
    #
    # Process and combine data for all commodities.
    combined = combine_data_for_all_commodities(supply_demand_data=supply_demand_data)

    # Create a table with metadata.
    tb = pr.read_from_df(data=combined, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True)

    # Columns contain a mix of numbers and strings (I suppose corresponding to footnotes).
    # For now, save all data as strings.
    tb = tb.astype({column: str for column in tb.columns})

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    # NOTE: There are duplicated rows with different data, e.g. Nickel 2019. We'll fix it in the garden step.
    tb = tb.format(["commodity", "year"], verify_integrity=False)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
