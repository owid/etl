"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ai_wrp_2021.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("wrp_2021"))

    # Read table from meadow dataset.
    tb = ds_meadow["wrp_2021"]

    #
    # Process data.
    #
    log.info("wrp_2021.harmonize_countries")
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    df = pd.DataFrame(tb)
    # List of column names to keep in DataFrame (q8 and q9 are AI related)
    select_cols = [
        "country",
        "year",
        "gender",
        "education",
        "income_5",
        "emp_2010",
        "agegroups4",
        "globalregion",
        "q8",
        "q9",
    ]

    # Filter DataFrame to keep only the AI related columns
    df = df[select_cols]

    # Map numerical values to categorical for certain columns in the DataFrame
    df = map_values(df)

    # List of columns to split the DataFrame by when calculating question responses
    columns_to_split_by = ["country", "gender", "education", "income_5", "emp_2010", "agegroups4", "globalregion"]

    # Dictionary to map response codes to labels for question 9
    dict_q9 = {
        1: "Mostly help",
        2: "Mostly harm",
        3: "Don't have an opinion",
        4: "Neither",
        98: "DK(help/harm)",
        99: "Refused(help/harm)",
    }

    # Dictionary to map response codes to labels for question 8
    dict_q8 = {1: "Yes, would feel safe", 2: "No, would not feel safe", 98: "DK(cars)", 99: "Refused(cars)"}

    # Create a list of DataFrames for each column_to_split_by for question 8
    df_q8_list = []
    for column in columns_to_split_by:
        df_q8_list.append(question_extract("q8", df, column, dict_q8))

    # Concatenate all the q8 DataFrames in the list to create a combined DataFrame
    df_q8_c = pd.concat(df_q8_list)
    df_q8_c.reset_index(drop=True, inplace=True)

    # Create a list of DataFrames for each column_to_split_by for question 9
    df_q9_list = []
    for column in columns_to_split_by:
        df_q9_list.append(question_extract("q9", df, column, dict_q9))

    # Concatenate all the q9 DataFrames in the list to create a combined DataFrame
    df_q9_c = pd.concat(df_q9_list)
    df_q9_c.reset_index(drop=True, inplace=True)

    # Merge the two combined DataFrames on common columns
    df_merge = pd.merge(df_q9_c, df_q8_c, on=columns_to_split_by + ["year"], how="outer")

    # Now split categories (gender, income etc) into separate columns
    # Copy df without categories (gender, income etc)
    df_without_categories = (
        df_merge[
            [
                "country",
                "year",
                "Yes, would feel safe",
                "Mostly help",
                "No, would not feel safe",
                "Mostly harm",
                "Neither",
                "DK(help/harm)",
                "Refused(help/harm)",
                "DK(cars)",
                "Refused(cars)",
                "Don't have an opinion",
            ]
        ]
        .dropna(subset=["country"])
        .copy()
    )

    merge_rest = calculate_world_data(df_merge, df_without_categories)

    tb = Table(merge_rest, short_name=paths.short_name, underscore=True)

    tb["dk_no_op"] = tb[["dk__help_harm", "dont_have_an_opinion"]].sum(axis=1).values
    tb["other_help_harm"] = tb[["dk__help_harm", "dont_have_an_opinion", "refused__help_harm"]].sum(axis=1).values
    tb["other_yes_no"] = tb[["dk__cars", "refused__cars"]].sum(axis=1).values
    tb[["dk_no_op", "other_help_harm", "other_yes_no"]] = tb[["dk_no_op", "other_help_harm", "other_yes_no"]].replace(
        0.0, np.NaN
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_wrp_2021.end")


def calculate_percentage(df, column, valid_responses_dict, column_to_split_by):
    """
    Calculates the percentage of valid responses for a given column in a DataFrame, split by another column.
    Args:
        df (DataFrame): The input DataFrame.
        column (str): The column name to calculate the percentage.
        valid_responses_dict (dict): A dictionary mapping vali
        d response codes to their corresponding labels.
        column_to_split_by (str): The column name to split by.
    Returns:
        DataFrame: A DataFrame with columns: the column_to_split_by, "year", "column", "count", and "percentage".
    """
    # Filter out invalid responses
    valid_responses = df[column].isin(valid_responses_dict.keys())
    df_filtered = df[[column_to_split_by, "year", column]][valid_responses].reset_index(drop=True)

    # Group by country and year
    grouped = df_filtered.groupby([column_to_split_by, "year"])

    # Count valid responses
    counts = grouped[column].value_counts().reset_index(name="count")

    # Calculate total counts for each country and year
    total_counts = counts.groupby([column_to_split_by, "year"])["count"].transform("sum")

    # Calculate percentage
    counts["percentage"] = (counts["count"] / total_counts) * 100
    # Map response codes to labels
    counts[column] = counts[column].map(valid_responses_dict)

    return counts


def question_extract(q, df, column_to_split_by, dict_q):
    """
    Computes the share of responses for a given column in a DataFrame, split by another column.
    Args:
        q (str): The question column name.
        df (DataFrame): The input DataFrame.
        column_to_split_by (str): The column name to split by.
        dict_q (dict): A dictionary mapping valid response codes to their corresponding labels.
    Returns:
        DataFrame: A DataFrame with columns: "year", column_to_split_by, and either "Mostly help" or "Yes, would feel safe" depending on the question column.
    """
    # Calculate percentage for worries about a terrorist attack
    counts_q = calculate_percentage(df, q, dict_q, column_to_split_by)

    # Select relevant columns
    select_df = counts_q[
        [
            column_to_split_by,
            "year",
            "percentage",
            q,
        ]
    ]

    # Pivot the DataFrame
    pivoted_df = select_df.pivot(
        index=[column_to_split_by, "year"],
        columns=q,
        values="percentage",
    )
    pivoted_df.reset_index(inplace=True)
    pivoted_df.columns.name = None

    if q == "q9":
        return pivoted_df[
            [
                "year",
                column_to_split_by,
                "Mostly help",
                "Mostly harm",
                "Neither",
                "Don't have an opinion",
                "DK(help/harm)",
                "Refused(help/harm)",
            ]
        ]
    else:
        return pivoted_df[
            ["year", column_to_split_by, "Yes, would feel safe", "No, would not feel safe", "DK(cars)", "Refused(cars)"]
        ]


def calculate_world_data(df_merge, df_without_categories):
    # Select rows with categories (NaN country rows)
    world_df = df_merge[df_merge["country"].isna()].copy()
    world_df.reset_index(drop=True, inplace=True)

    # Set country as World
    world_df["country"] = world_df["country"].astype(str)
    world_df.loc[world_df["country"] == "nan", "country"] = "World"

    # Calculate the percentage of valid responses for "Mostly help", "Mostly harm", "Neither" in a DataFrame,
    # split by gender, income etc.
    columns_to_calculate = [
        "Mostly help",
        "Mostly harm",
        "Neither",
        "DK(help/harm)",
        "Don't have an opinion",
        "Refused(help/harm)",
    ]
    merge_help_harm_all = None
    for column in columns_to_calculate:
        conc_df = pivot_by_category(world_df, column)
        if merge_help_harm_all is None:
            merge_help_harm_all = conc_df
        else:
            merge_help_harm_all = pd.merge(merge_help_harm_all, conc_df, on=["year", "country"], how="outer")

    # Calculate the percentage of valid responses for "Yes, would feel safe" in a DataFrame, split by gender, income etc.
    columns_to_calculate = ["Yes, would feel safe", "No, would not feel safe", "DK(cars)", "Refused(cars)"]
    merge_yes_no = None
    for column in columns_to_calculate:
        conc_df = pivot_by_category(world_df, column)
        if merge_yes_no is None:
            merge_yes_no = conc_df
        else:
            merge_yes_no = pd.merge(merge_yes_no, conc_df, on=["year", "country"], how="outer")

    # Merge all dataframes into one
    merge_categorized = pd.merge(merge_help_harm_all, merge_yes_no, on=["year", "country"], how="outer")
    merge_rest = pd.merge(df_without_categories, merge_categorized, on=["year", "country"], how="outer")

    merge_rest.set_index(["year", "country"], inplace=True)
    return merge_rest


def map_values(df):
    """
    Maps numerical values to categorical for certain columns in the DataFrame.
    Args:
        df (DataFrame): The input DataFrame.
    Returns:
        DataFrame: The DataFrame with mapped values for "gender", "education", "income_5", "emp_2010", "agegroups4", "globalregion" columns.
    """
    gender = {1: "Male", 2: "Female"}

    education_level = {
        1: "Primary (0-8 years)",
        2: "Secondary (9-15 years)",
        3: "Tertiary (16 years or more)",
    }

    wealth_quintile = {1: "Poorest 20%", 2: "Second 20%", 3: "Middle 20%", 4: "Fourth 20%", 5: "Richest 20%"}

    employment_status = {
        1: "Employed full time for an employer",
        2: "Employed full time for self",
        3: "Employed part time do not want full time",
        4: "Unemployed",
        5: "Employed part time want full time",
        6: "Out of workforce",
    }

    age_group = {1: "15-29", 2: "30-49", 3: "50-64", 4: "65+"}

    region = {
        1: "Eastern Africa",
        2: "Central/Western Africa",
        3: "North Africa",
        4: "Southern Africa",
        5: "Latin America & Caribbean",
        6: "Northern America",
        7: "Central Asia",
        8: "East Asia",
        9: "South-eastern Asia",
        10: "South Asia",
        11: "Middle East",
        12: "Eastern Europe",
        13: "Northern/Western Europe",
        14: "Southern Europe",
        15: "Australia and New Zealand",
    }

    df["gender"] = df["gender"].map(gender)
    df["education"] = df["education"].map(education_level)
    df["income_5"] = df["income_5"].map(wealth_quintile)
    df["emp_2010"] = df["emp_2010"].map(employment_status)
    df["agegroups4"] = df["agegroups4"].map(age_group)
    df["globalregion"] = df["globalregion"].map(region)

    return df


def pivot_by_category(df, question):
    """
    Pivot the input DataFrame by categories and a specific question.

    Parameters:
        df (DataFrame): Input DataFrame to pivot.
        question (str): The specific question to pivot on.

    Returns:
        DataFrame: Concatenated pivot tables with suffixed column names.

    """
    # Create an empty list to store the pivot tables
    pivot_tables = []
    cols_pivot = ["gender", "education", "income_5", "emp_2010", "agegroups4", "globalregion"]

    # Iterate over each pivot column
    for pivot_col in cols_pivot:
        # Pivot the dataframe for the current pivot column
        pivoted_df = pd.pivot_table(df, values=question, index=["country", "year"], columns=pivot_col)
        # Append the pivot table to the list
        pivot_tables.append(pivoted_df)

    # Concatenate all the pivot tables along the columns
    conc_df = pd.concat(pivot_tables, axis=1)

    # Add suffix to column names
    conc_df = conc_df.add_suffix("_" + question)

    return conc_df
