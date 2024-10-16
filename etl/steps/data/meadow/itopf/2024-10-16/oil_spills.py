"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd
import pdfplumber
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("oil_spills.pdf")

    #
    # Process data.
    #

    # Open and process the PDF fil
    with pdfplumber.open(snap.path) as pdf:
        # extract oil spilled
        quantity_text = pdf.pages[6].extract_text()
        df_oil_spilled = extract_spill_quantity(quantity_text)

        assert df_oil_spilled.index.is_unique, "Index is not unique' for quantity of oil spilled."
        df_oil_spilled.reset_index(inplace=True)

        # extract number of spills
        number_text = pdf.pages[5].extract_text()
        df_nspills = extract_spill_number(number_text)
        assert df_nspills.index.is_unique, "Index is not unique for oil spills'."
        df_nspills.reset_index(inplace=True)

        # extract number of spills
        text_cause = pdf.pages[15].extract_text()
        df_7_7000, df_above_7000 = extract_cause_data(text_cause)

    # Extract and merge oil spilled and number of spills
    nsp_quant = pd.merge(df_nspills, df_oil_spilled, on="year", how="outer")
    nsp_quant["spill_type"] = "World"  #  World

    df_above_7000_cause_totals_pv = extract_cause_totals(df_above_7000, 2023, "Large (>700t)")
    df_below_7000_cause_totals_pv = extract_cause_totals(df_7_7000, 2023, "Medium (7-700t)")
    # Concatenate the two pivoted DataFrames along the row axis
    merged_causes = pd.concat([df_above_7000_cause_totals_pv, df_below_7000_cause_totals_pv], axis=0)
    # Append suffix '_causes' to non 'year' and 'spill_type' columns
    for column in merged_causes.columns:
        if column not in ["year", "spill_type"]:
            merged_causes = merged_causes.rename(columns={column: column + " causes"})

    ops_df = extract_operations(df_above_7000, df_7_7000)  # Extract operationd during which oil spills
    # Merge operations_total with merged_causes on 'year' and 'spill_type'
    merge_cause_op = pd.merge(merged_causes, ops_df, on=["year", "spill_type"])
    # Merge nsp_quant (oil spilled and number of spills) with merge_cause_op (causes and operation type of indcidents) on 'year' and 'spill_type'
    combined_df = pd.merge(nsp_quant, merge_cause_op, on=["year", "spill_type"], how="outer")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(combined_df, short_name=paths.short_name, underscore=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["spill_type", "year"])
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def extract_spill_quantity(text):
    """
    Extracts oil spill quantity data from the given text (assumes the 2024 version of the table).

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
    df_oil_spills = df_oil_spills.sort_values("year")

    # Remove the row where year is 2023 and the value is 7 - this is just a page number
    df_oil_spills = df_oil_spills[(df_oil_spills["year"] != 2023) | (df_oil_spills["oil_spilled"] != 7)]
    df_oil_spills = df_oil_spills.set_index("year")

    return df_oil_spills


def extract_spill_number(text):
    """
    Extracts number of oil spills data from the given text (assumes the 2024 version of the table).

    Args:
        text (str): The text containing spill data.

    Returns:
        pd.DataFrame: DataFrame with extracted spill data.
    """

    # Define pattern to extract number of spills data
    pattern = r"(?<!\d)(19[7-9]\d|20[0-1]\d|2020|2021|2022|2023)(?:\s+(\d+)\s+(\d+))?"

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

    df_spills = df_spills.set_index("year")

    return df_spills


def extract_cause_data(text):
    """
    Extracts the cause data from the given text (assumes the 2024 version of the table).

    Args:
        text (str): The text containing cause data.

    Returns:
        pd.DataFrame: Dataframe with extracted cause data.
    """
    names = [
        "ALLISION/",
        "GROUNDING",
        "HULL FAILURE",
        "EQUIPMENT",
        "FIRE/EXPLOSION",
        "OTHER",
        "UNKNOWN",
    ]

    split_text = re.split(r"TABLE 4:[\s\S]+?\n[A-Za-z0-9 ]", text)
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
    # Updated cause names
    cause_mapping = {
        "ALLISION/": "Allision/Collision",
        "GROUNDING": "Grounding",
        "HULL FAILURE": "Hull Failure",
        "EQUIPMENT": "Equipment Failure",
        "FIRE/EXPLOSION": "Fire/Explosion",
        "OTHER": "Other",
        "UNKNOWN": "Unknown",
    }

    # Rename the values in the 'Cause' column
    df_7_7000["Cause"] = df_7_7000["Cause"].map(cause_mapping)
    df_above_7000["Cause"] = df_above_7000["Cause"].map(cause_mapping)

    return df_7_7000, df_above_7000


def prepare_ops_dataframe(df, spill_type):
    """
    Prepares a dataframe of oil spill operations by summarizing and extracting relevant information.

    This function performs several tasks:
        1. Converts columns (from the second onwards) to integers.
        2. Creates a new row, 'Operations Total', which is the sum of all rows in the dataframe.
        3. Extracts the last row from the dataframe.
        4. Adds 'spill_type' and 'year' columns to the dataframe, assigning constant values.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas dataframe containing oil spill operation data.
        Expected to have at least one row and multiple columns where the second column onwards represent numeric data.

    spill_type : str
        A string representing the category of oil spills (e.g., 'Large (>700t)' or 'Medium (7-700t)').

    Returns
    -------
    operations : pd.DataFrame
        A single-row dataframe containing the total operations, the assigned spill_type, and the year.
        The dataframe maintains the same columns as the input dataframe, with the addition of 'spill_type' and 'year'.

    Note
    ----
    The function assigns a constant year (2023) for all operations.
    """
    # Convert columns from the second one onwards to integers
    df[df.columns[1:]] = df[df.columns[1:]].astype(int)

    # Create a new row 'Operations Total' in df that is the sum of all rows
    df.loc["Operations Total"] = df.sum(axis=0)

    # Extract the last row from df
    operations = df.iloc[[-1]].copy()

    # Assign constant values to 'spill_type' and 'year'
    operations["spill_type"] = spill_type
    operations["year"] = 2023

    return operations


def extract_operations(df_above_7000, df_7_7000):
    """
    Extracts oil spill operations from two dataframes and returns a combined dataframe.

    The function uses the `prepare_ops_dataframe` function to summarize and extract the operations data
    from two different dataframes: one for oil spills above 7000 tons and one for oil spills between 7 and 7000 tons.
    After preparing the data (calls prepare_ops_dataframe function), it concatenates the resulting dataframes into a single dataframe and appends
    the suffix '_ops' to the column names (except for 'year' and 'spill_type'). Finally, it removes the column "Cause_ops".

    Parameters
    ----------
    df_above_7000 : pd.DataFrame
        A pandas dataframe containing data about oil spill operations above 7000 tons.

    df_7_7000 : pd.DataFrame
        A pandas dataframe containing data about oil spill operations between 7 and 7000 tons.

    Returns
    -------
    operations_total : pd.DataFrame
        A pandas dataframe containing the total operations from both input dataframes, with renamed columns and
        unnecessary columns removed.
    Note
    ----
    """
    operations_ab_7000 = prepare_ops_dataframe(df_above_7000, "Large (>700t)")
    operations_bel_7000 = prepare_ops_dataframe(df_7_7000, "Medium (7-700t)")
    # Concatenate the last rows from both dataframes into a new dataframe
    operations_total = pd.concat([operations_bel_7000, operations_ab_7000])

    # Append suffix '_causes' to non 'year' and 'country' columns
    for column in operations_total.columns:
        if column not in ["year", "spill_type"]:
            operations_total = operations_total.rename(columns={column: column + " ops"})

    return operations_total


def extract_cause_totals(df, year, spill_type):
    """
    Extracts total cause data from a given DataFrame, reformats it, and returns the processed DataFrame.

    This function takes a DataFrame, copies relevant columns, assigns a constant 'year' and 'spill_type'
    to the data, reshapes it by pivoting on the 'year', sets 'Cause' as columns, and 'Total' as values.
    It then resets the DataFrame index to flatten it and assigns a constant spill_type value.

    Parameters
    ----------
    df : pd.DataFrame
        The input pandas DataFrame to process.

    year : int
        The year to assign to the 'year' column.

    spill_type : str
        The spill_type to assign to the 'spill_type' column.

    Returns
    -------
    df_cause_totals_pv : pd.DataFrame
        The processed DataFrame, which includes the total causes data, now reshaped and assigned with
        a 'year' and 'spill_type'.
    """
    # Copy specific columns from DataFrame
    df_cause_totals = df[["Cause", df.columns[-1]]].copy()
    # Assign a constant year value to the 'year' column for DataFrame
    df_cause_totals["year"] = year
    # Pivot DataFrame to reshape it, setting 'year' as the index, 'Cause' as the columns, and 'Total' as the values
    df_cause_totals_pv = df_cause_totals.pivot(index="year", columns="Cause", values="Total")
    # Reset index
    df_cause_totals_pv.reset_index(inplace=True)
    # Assign a constant spill_type value to the 'spill_type' column for pivoted DataFrame
    df_cause_totals_pv["spill_type"] = spill_type
    return df_cause_totals_pv
