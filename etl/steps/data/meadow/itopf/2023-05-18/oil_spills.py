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

    causes_df = extract_causes_of_oil_spills(df_above_7000, df_7_7000)  # Extract causes of oil spills
    ops_df = extract_operations(df_above_7000, df_7_7000)  # Extract operationd during which oil spills

    # Merge operations_total with merged_causes on 'year' and 'country'
    merge_cause_op = pd.merge(
        causes_df, ops_df, on=["year", "country"]
    )  # country actually just means "Large (>700t)" or "Medium (7-700t)" oil spills

    # Merge nsp_quant (oil spilled and number of spills) with merge_cause_op (causes and operation type of indcidents) on 'year' and 'country'
    combined_df = pd.merge(nsp_quant, merge_cause_op, on=["year", "country"], how="outer")

    # Drop the 'Total_ops' column from combined_df (as we aren't going to use it (total number of spills for all causes)
    combined_df.drop("Total ops", axis=1, inplace=True)

    # Merge combined_df with df_biggest_spills (biggest spills between 1970-2022) on 'year' and 'country'
    merge_biggest_spills = pd.merge(combined_df, df_biggest_spills, on=["year", "country"], how="outer")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(merge_biggest_spills, short_name=paths.short_name, underscore=True)

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("oil_spills.end")


def prepare_ops_dataframe(df, country):
    """
    Prepares a dataframe of oil spill operations by summarizing and extracting relevant information.

    This function performs several tasks:
        1. Converts columns (from the second onwards) to integers.
        2. Creates a new row, 'Operations Total', which is the sum of all rows in the dataframe.
        3. Extracts the last row from the dataframe.
        4. Adds 'country' and 'year' columns to the dataframe, assigning constant values.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas dataframe containing oil spill operation data.
        Expected to have at least one row and multiple columns where the second column onwards represent numeric data.

    country : str
        A string representing the category of oil spills (e.g., 'Large (>700t)' or 'Medium (7-700t)').

    Returns
    -------
    operations : pd.DataFrame
        A single-row dataframe containing the total operations, the assigned country, and the year.
        The dataframe maintains the same columns as the input dataframe, with the addition of 'country' and 'year'.

    Note
    ----
    The 'country' label is used here to categorize large and medium oil spills, a naming trick used for visualization.
    The function assigns a constant year (2023) for all operations.
    """
    # Convert columns from the second one onwards to integers
    df[df.columns[1:]] = df[df.columns[1:]].astype(int)

    # Create a new row 'Operations Total' in df that is the sum of all rows
    df.loc["Operations Total"] = df.sum(axis=0)

    # Extract the last row from df
    operations = df.iloc[[-1]].copy()

    # Assign constant values to 'country' and 'year'
    operations["country"] = country
    operations["year"] = 2022

    return operations


def extract_operations(df_above_7000, df_7_7000):
    """
    Extracts oil spill operations from two dataframes and returns a combined dataframe.

    The function uses the `prepare_ops_dataframe` function to summarize and extract the operations data
    from two different dataframes: one for oil spills above 7000 tons and one for oil spills between 7 and 7000 tons.
    After preparing the data (calls prepare_ops_dataframe function), it concatenates the resulting dataframes into a single dataframe and appends
    the suffix '_ops' to the column names (except for 'year' and 'country'). Finally, it removes the column "Cause_ops".

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
    The 'country' label is used here to categorize large and medium oil spills, a naming trick used for visualization.
    """
    operations_ab_7000 = prepare_ops_dataframe(df_above_7000, "Large (>700t)")
    operations_bel_7000 = prepare_ops_dataframe(df_7_7000, "Medium (7-700t)")
    # Concatenate the last rows from both dataframes into a new dataframe
    operations_total = pd.concat([operations_bel_7000, operations_ab_7000])
    operations_total = append_suffix_to_non_year_country_columns(operations_total, " ops")
    del operations_total["Cause ops"]

    return operations_total


def extract_causes_of_oil_spills(df_above_7000, df_below_7000):
    """
    Extracts the causes of oil spills from two dataframes and returns a combined dataframe.

    This function calls the `extract_cause_totals` function to extract the causes of oil spills from
    two different dataframes: one for oil spills above 7000 tons and one for oil spills below 7000 tons.
    After preparing the data, it concatenates the resulting dataframes into a single dataframe and appends
    the suffix '_causes' to the column names (except for 'year' and 'country').

    Parameters
    ----------
    df_above_7000 : pd.DataFrame
        A pandas dataframe containing data about causes of oil spills above 7000 tons.

    df_below_7000 : pd.DataFrame
        A pandas dataframe containing data about causes of oil spills below 7000 tons.

    Returns
    -------
    merged_causes : pd.DataFrame
        A pandas dataframe containing the total causes from both input dataframes, with columns renamed to append '_causes'.
    """
    df_above_7000_cause_totals_pv = extract_cause_totals(df_above_7000, 2022, "Large (>700t)")
    df_below_7000_cause_totals_pv = extract_cause_totals(df_below_7000, 2022, "Medium (7-700t)")
    # Concatenate the two pivoted DataFrames along the row axis
    merged_causes = pd.concat([df_above_7000_cause_totals_pv, df_below_7000_cause_totals_pv], axis=0)
    # Append suffix '_causes' to non 'year' and 'country' columns
    merged_causes = append_suffix_to_non_year_country_columns(merged_causes, " causes")
    return merged_causes


def extract_cause_totals(df, year, country):
    """
    Extracts total cause data from a given DataFrame, reformats it, and returns the processed DataFrame.

    This function takes a DataFrame, copies relevant columns, assigns a constant 'year' and 'country'
    to the data, reshapes it by pivoting on the 'year', sets 'Cause' as columns, and 'Total' as values.
    It then resets the DataFrame index to flatten it and assigns a constant country value.

    Parameters
    ----------
    df : pd.DataFrame
        The input pandas DataFrame to process.

    year : int
        The year to assign to the 'year' column.

    country : str
        The country to assign to the 'country' column.

    Returns
    -------
    df_cause_totals_pv : pd.DataFrame
        The processed DataFrame, which includes the total causes data, now reshaped and assigned with
        a 'year' and 'country'.
    """
    # Copy specific columns from DataFrame
    df_cause_totals = df[["Cause", df.columns[-1]]].copy()
    # Assign a constant year value to the 'year' column for DataFrame
    df_cause_totals["year"] = year
    # Pivot DataFrame to reshape it, setting 'year' as the index, 'Cause' as the columns, and 'Total' as the values
    df_cause_totals_pv = df_cause_totals.pivot(index="year", columns="Cause", values="Total")
    # Reset index
    df_cause_totals_pv.reset_index(inplace=True)
    # Assign a constant country value to the 'country' column for pivoted DataFrame
    df_cause_totals_pv["country"] = country
    return df_cause_totals_pv


def append_suffix_to_non_year_country_columns(df, suffix):
    """
    Appends a provided suffix to the names of all columns in a DataFrame that are not 'year' or 'country'.

    This function iterates over all column names in the input DataFrame. If the column name is not
    'year' or 'country', it appends the provided suffix to the column name.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose columns names need to be processed.

    suffix : str
        The suffix to be appended to each column name that is not 'year' or 'country'.

    Returns
    -------
    df : pd.DataFrame
        The DataFrame with processed column names.
    """
    for column in df.columns:
        if column not in ["year", "country"]:
            df.rename(columns={column: column + suffix}, inplace=True)
    return df


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
