"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd
import pdfplumber
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("oil_spills.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("oil_spills.pdf")
    # Open and process the PDF fil
    with pdfplumber.open(snap.path) as pdf:
        # extract oil spilled
        text = extract_text_from_page(pdf, 6)
        df_oil_spilled = extract_spill_quantity(text)

        assert df_oil_spilled.index.is_unique, "Index is not unique' for quantity of oil spilled."
        df_oil_spilled.reset_index(inplace=True)

        # extract number of spills
        text_number = extract_text_from_page(pdf, 5)
        df_nspills = extract_spill_number(text_number)
        assert df_nspills.index.is_unique, "Index is not unique for oil spills'."
        df_nspills.reset_index(inplace=True)

        # extract biggest spills in history (not currently used; just in case we want it at some point)
        text_mj = extract_text_from_page(pdf, 4)
        df_biggest_spills = extract_biggest_spills(text_mj)
        df_biggest_spills.reset_index(inplace=True)

        # extract cause and operations data
        text_cause = extract_text_from_page(pdf, 15)
        df_above_7000, df_7_7000 = extract_cause_data(text_cause)

    # Extract and merge oil spilled and number of spills
    nsp_quant = pd.merge(df_nspills, df_oil_spilled, on="year", how="outer")
    nsp_quant["country"] = "World"  # add World

    # Extract causes of oil spills
    # Copy specific columns from two different DataFrames
    df_above_7000_cause_totals = df_above_7000[["Cause", df_above_7000.columns[-1]]].copy()
    df_below_7000_cause_totals = df_7_7000[["Cause", df_7_7000.columns[-1]]].copy()

    # Assign a constant year value to the 'year' column for both DataFrames
    df_below_7000_cause_totals["year"] = 2023
    df_above_7000_cause_totals["year"] = 2023

    # Pivot both DataFrames to reshape them, setting 'year' as the index, 'Cause' as the columns, and 'Total' as the values
    df_below_7000_cause_totals_pv = df_below_7000_cause_totals.pivot(index="year", columns="Cause", values="Total")
    df_above_7000_cause_totals_pv = df_above_7000_cause_totals.pivot(index="year", columns="Cause", values="Total")

    # Remove column name for the index for both pivoted DataFrames
    df_below_7000_cause_totals_pv = df_below_7000_cause_totals_pv.rename_axis(None, axis="columns")
    df_above_7000_cause_totals_pv = df_above_7000_cause_totals_pv.rename_axis(None, axis="columns")

    # Reset index for both pivoted DataFrames
    df_below_7000_cause_totals_pv.reset_index(inplace=True)
    df_above_7000_cause_totals_pv.reset_index(inplace=True)

    # Assign a constant country value to the 'country' column for both pivoted DataFrames
    df_below_7000_cause_totals_pv["country"] = "Small (7-700t)"
    df_above_7000_cause_totals_pv["country"] = "Large (>700t)"

    # Concatenate the two pivoted DataFrames along the row axis
    merged_causes = pd.concat([df_above_7000_cause_totals_pv, df_below_7000_cause_totals_pv], axis=0)

    # For every column in the merged DataFrame that is not 'year' or 'country', rename the column by appending '_causes' to the existing column name
    for column in merged_causes.columns:
        if column not in ["year", "country"]:
            merged_causes.rename(columns={column: column + "_causes"}, inplace=True)

    # Extract operations during which spills occurred
    # Convert columns from the second one onwards in df_above_7000 to integers
    df_above_7000[df_above_7000.columns[1:]] = df_above_7000[df_above_7000.columns[1:]].astype(int)

    # Create a new row 'Operations Total' in df_above_7000 that is the sum of all rows
    df_above_7000.loc["Operations Total"] = df_above_7000.sum(axis=0)

    # Extract the last row from df_above_7000
    operations_ab_7000 = df_above_7000.iloc[[-1]]

    # Convert columns from the second one onwards in df_7_7000 to integers
    df_7_7000[df_7_7000.columns[1:]] = df_7_7000[df_7_7000.columns[1:]].astype(int)

    # Create a new row 'Operations Total' in df_7_7000 that is the sum of all rows
    df_7_7000.loc["Operations Total"] = df_7_7000.sum(axis=0)

    # Extract the last row from df_7_7000
    operations_bel_7000 = df_7_7000.iloc[[-1]]

    # Concatenate the last rows from df_above_7000 and df_7_7000 into a new dataframe
    operations_total = pd.merge(operations_bel_7000, operations_ab_7000, how="outer")
    # Rename the Cause column to country of operations_total to 'Small (7-700t)' and 'Large (>700t)'
    operations_total.at[0, "country"] = "Small (7-700t)"
    operations_total.at[1, "country"] = "Large (>700t)"
    del operations_total["Cause"]

    # Add a new column 'year' to operations_total with a constant value 2023
    operations_total["year"] = 2023

    # Append '_ops' to each column name that is not 'year' or 'country' in operations_total
    for column in operations_total.columns:
        if column not in ["year", "country"]:
            operations_total.rename(columns={column: column + "_ops"}, inplace=True)

    # Merge operations_total with merged_causes on 'year' and 'country'
    merge_cause_op = pd.merge(merged_causes, operations_total, on=["year", "country"])

    # Merge nsp_quant with merge_cause_op on 'year' and 'country' into a new dataframe 'combined_df'
    combined_df = pd.merge(nsp_quant, merge_cause_op, on=["year", "country"], how="outer")

    # Drop the 'Total_ops' column from combined_df
    combined_df.drop("Total_ops", axis=1, inplace=True)

    # Merge combined_df with df_biggest_spills on 'year' and 'country'
    merge_biggest_spills = pd.merge(combined_df, df_biggest_spills, on=["year", "country"], how="outer")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(merge_biggest_spills, short_name=paths.short_name, underscore=True)

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("oil_spills.end")


def extract_text_from_page(pdf, page_number):
    """
    Extracts the text from a specific page of a PDF.

    Args:
        pdf (pdfplumber.PDF): The PDF object.
        page_number (int): The page number to extract text from.

    Returns:
        str: The extracted text.
    """
    page = pdf.pages[page_number]
    return page.extract_text()


def extract_spill_quantity(text):
    """
    Extracts oil spill quantity data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing oil spill data.

    Returns:
        pd.DataFrame: DataFrame with extracted oil spill data.
    """

    # Define pattern to extract oil spill quantity
    pattern = r"(?<!\d)(?:19[7-9]\d|20[0-2]\d)\s+(\d{1,3}(?:,\d{3})*)"

    # Extract matches of oil spill quantity
    matches = re.findall(pattern, text)

    # Define pattern to extract year
    pattern_year = r"\b(19[7-9]\d|20[0-2]\d)\b(?=(?:\s+\d{1,3}(?:,\d{3})*)\b)"

    # Extract matches of year
    matches_year = re.findall(pattern_year, text)

    # Convert year matches to integer
    years = [int(year) for year in matches_year]

    # Convert oil spill quantity matches to integer
    oil_spilled = [int(oil.replace(",", "")) for oil in matches]

    # Create DataFrame with extracted oil spill data
    df_oil_spills = pd.DataFrame({"year": years, "oil_spilled": oil_spilled})
    df_oil_spills.sort_values("year", inplace=True)
    df_oil_spills.set_index("year", inplace=True)

    return df_oil_spills


def extract_spill_number(text):
    """
    Extracts number of oil spills data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing spill data.

    Returns:
        pd.DataFrame: DataFrame with extracted spill data.
    """

    # Define pattern to extract number of spills data
    pattern = r"(?<!\d)(19[7-9]\d|20[0-1]\d|2020|2021|2022)(?:\s+(\d+)\s+(\d+))?"

    # Extract matches of number of spills data
    matches = re.findall(pattern, text)

    data = []
    for match in matches:
        year = match[0]
        value1 = match[1]
        value2 = match[2]

        if value1 != "" and value2 != "":
            data.append([year, value1, value2])

    # Create DataFrame with extracted number of spills data
    df_spills = pd.DataFrame(data, columns=["year", "bel_700t", "ab_700t"])
    df_spills.sort_values("year", inplace=True)

    # Convert columns to integer type
    for column in df_spills.columns:
        df_spills[column] = df_spills[column].astype(int)

    df_spills.set_index("year", inplace=True)

    return df_spills


def extract_biggest_spills(text):
    """
    Extracts the biggest spills data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing biggest spills data.

    Returns:
        pd.DataFrame: Dataframe with extracted biggest spills data.
    """
    pattern = r"\n(\d+)\s(.+?)\s+(\d{4})\s(.+?)\s(\d{1,3}(?:,\d{3})*)"
    matches = re.findall(pattern, text)

    years = []
    locations = []
    spill_sizes = []

    for match in matches:
        year = int(match[2])
        location = match[3]
        spill_size = match[4].replace(",", "")

        years.append(year)
        locations.append(location)
        spill_sizes.append(spill_size)

    df_biggest_spills = pd.DataFrame({"year": years, "country": locations, "biggest_spills_size": spill_sizes})
    df_biggest_spills["biggest_spills_size"] = df_biggest_spills["biggest_spills_size"].astype(int)

    df_biggest_spills.set_index("year", inplace=True)

    return df_biggest_spills


def extract_cause_data(text):
    """
    Extracts the cause data from the given text (assumes the 2023 version of the table).

    Args:
        text (str): The text containing cause data.

    Returns:
        pd.DataFrame: Dataframe with extracted cause data.
    """
    names = [
        "Allision/Collision",
        "Grounding",
        "Hull Failure",
        "Equipment Failure",
        "Fire/Explosion",
        "Other",
        "Unknown",
    ]

    split_text = re.split(r"Table 4:[\s\S]+?\n[A-Za-z0-9 ]", text)
    before_table4 = split_text[0]
    after_table4 = split_text[1]

    name_number_pairs_4 = []
    name_number_pairs_5 = []

    for name in names:
        pattern = r"({})\s+(\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+)".format(name)
        match = re.search(pattern, before_table4)
        if match:
            numbers = match.group(2).split()
            name_number_pairs_4.append((name, *numbers))

    for name in names:
        pattern = r"({})\s+(\d+\s+\d+\s+\d+\s+\d+\s+\d+)".format(name)
        match = re.search(pattern, after_table4)
        if match:
            numbers = match.group(2).split()
            name_number_pairs_5.append((name, *numbers))

    df_above_7000 = pd.DataFrame(
        name_number_pairs_4,
        columns=[
            "Cause",
            "At Anchor (inland)",
            "At Anchor (open Water)",
            "Underway (inland)",
            "Underway (open water)",
            "Loading/Discharing",
            "Other Operations",
            "Total",
        ],
    )
    df_7_7000 = pd.DataFrame(
        name_number_pairs_5, columns=["Cause", "Loading/Discharing", "Bunkering", "Other Operations", "Uknown", "Total"]
    )

    return df_above_7000, df_7_7000
