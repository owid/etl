"""Load a snapshot and create a meadow dataset."""

import difflib

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_data(snap: Snapshot):
    """
    Load the Excel file from the given snapshot.

    Args:
        snap (Snapshot): The snapshot object containing the path to the Excel file.

    Returns:
        pd.ExcelFile: The loaded Excel file as a pandas ExcelFile object, or None if loading failed.
    """

    # Attempt to load the Excel file from the snapshot path.
    try:
        excel_object = pd.ExcelFile(snap.path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Excel file not found at path: {snap.path}")
    except IsADirectoryError:
        raise IsADirectoryError(f"Provided path is a directory, not an Excel file: {snap.path}")
    except Exception as e:
        raise Exception(f"An error occurred while loading the Excel file: {e}")

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object


def process_sheet(excel_object: pd.ExcelFile, sheet_name: str, year_range: tuple) -> pd.DataFrame:
    """
    Process a sheet from the given Excel file and return a cleaned DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
        sheet_name (str): The name of the sheet to process from the Excel file.
        year_range (tuple): A tuple with 2 elements (start_year, end_year) indicating the years to include in the output DataFrame.

    Returns:
        pd.DataFrame: The cleaned and processed DataFrame.
    """

    # Read the sheet from the Excel file
    df = pd.read_excel(excel_object, sheet_name=sheet_name, header=5)
    # Drop unnecessary columns
    df = df.drop(df.columns[:3], axis=1)

    # Drop additional columns
    columns_to_drop = ["Units", "Notes", "Series"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # More column dropping
    df = df.drop(df.columns[1], axis=1)
    df = df.drop(df.columns[-1], axis=1)

    # Remove rows and columns with all NaN values
    df.dropna(how="all", axis=1, inplace=True)
    df.dropna(how="all", axis=0, inplace=True)

    # Rename the 'Basic data and indicators' column to 'country'
    df = df.rename(columns={"Basic data and indicators": "country"})

    # Create the list of years to include in the output DataFrame
    years = [year for year in range(year_range[0], year_range[1])]
    non_year_cols = [col for col in df.columns if col not in years]

    # Melt the DataFrame to a long format
    df = df.melt(id_vars=non_year_cols, value_vars=years, var_name="year")

    # Fill missing country names with the previous valid value
    df["country"] = df["country"].ffill()

    # Drop rows with all NaN values in columns other than 'country', 'year', and 'value'
    df.dropna(subset=df.columns.difference(["country", "year", "value"]), how="all", inplace=True)
    if sheet_name in ["Inbound Tourism-Accommodation", "Domestic Tourism-Accommodation"]:
        cols_to_fill = [col for col in df.columns if col not in ["country", "year", "value", "Unnamed: 6"]]
        df[cols_to_fill] = df[cols_to_fill].ffill()

    # Combine remaining columns to create the 'indicator' column
    df["indicator"] = df.drop(columns=["country", "value", "year"]).apply(
        lambda x: ",".join(x.dropna().astype(str)), axis=1
    )
    df.dropna(subset=["value"], inplace=True)

    # Keep only the necessary columns
    cols_to_keep = ["country", "year", "value", "indicator"]
    df = df[cols_to_keep]

    # Drop rows with missing 'value' and replace '..' with NaN
    df.dropna(subset=["value"], inplace=True)
    df["value"] = df["value"].replace("..", np.nan)

    # Add the sheet name to the 'indicator' column
    df["indicator"] = sheet_name + "-" + df["indicator"].astype(str)

    # Set the index to 'country', 'year', and 'indicator'
    df.set_index(["country", "year", "indicator"], inplace=True)

    assert (
        df.index.is_unique
    ), f"Index is not unique in sheet '{sheet_name}'."  # Added assert statement to check index is unique
    return df


def process_data(excel_object: pd.ExcelFile, year_range: tuple, matched_sheet_names: list) -> pd.DataFrame:
    """
    Process sheets of interest in the given Excel file and return a combined DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
        year_range (tuple): A tuple with 2 elements (start_year, end_year) indicating the years to include in the output DataFrame.

    Returns:
        pd.DataFrame: The combined and processed DataFrame from all sheets.
    """

    data_frames = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(matched_sheet_names):
        print(f"Processing sheet: {sheet_name}")
        df = process_sheet(excel_object, sheet_name, year_range)
        data_frames.append(df)

    # Concatenate all the processed DataFrames
    df_concat = pd.concat(data_frames, axis=0)
    df_concat = df_concat.astype({"value": float})
    df_concat.reset_index(inplace=True)

    # Pivot the DataFrame to have 'indicator' as columns and 'value' as cell values
    df_concat = df_concat.pivot_table(index=["country", "year"], columns="indicator", values="value", dropna=False)
    df_concat.reset_index(inplace=True)

    assert df_concat.index.is_unique, "The index in the concatenated DataFrame is not unique."
    return df_concat


def run(dest_dir: str) -> None:
    log.info("unwto.start")
    # Year range
    year_range = (1995, 2022)

    # Load inputs.
    snap: Snapshot = paths.load_dependency("unwto.xlsx")
    excel_object = load_data(snap)

    if excel_object is None:
        return

    # Get the list of sheet names in the Excel file
    sheet_names = excel_object.sheet_names
    log.info(f"Found {len(sheet_names)} sheets in the Excel file:")

    sheet_names_to_load = [
        "Inbound Tourism-Arrivals",
        "Inbound Tourism-Regions",
        "Inbound Tourism-Purpose",
        "Inbound Tourism-Transport",
        "Inbound Tourism-Accommodation",
        "Inbound Tourism-Expenditure",
        "Domestic Tourism-Trips",
        "Domestic Tourism-Accommodation",
        "Outbound Tourism-Departures",
        "Tourism Industries",
        "Employment",
        "Inbound Tourism-Expenditure",
        "Outbound Tourism-Expenditure",
    ]

    log.info(f"Loading {len(sheet_names_to_load)} sheets from the Excel file:")

    # Match the sheet names to load to the available sheet names
    matched_sheet_names = []
    for target_sheet_name in sheet_names_to_load:
        best_match = None
        best_ratio = 0
        for sheet_name in sheet_names:
            ratio = difflib.SequenceMatcher(None, target_sheet_name, sheet_name).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = sheet_name
        if best_ratio >= 0.6:  # You can adjust this threshold based on your requirements
            matched_sheet_names.append(best_match)

    # Print matched sheets
    for name in matched_sheet_names:
        print(f"- {name}")

    # Process data.
    df_concat = process_data(excel_object, year_range, matched_sheet_names)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_concat, short_name=paths.short_name, underscore=True)

    # Save outputs.
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("unwto.end")
