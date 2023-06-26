"""Load a meadow dataset and create a garden dataset."""

from typing import cast

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

    df = df[select_cols]

    df = map_values(df)
    columns_to_split_by = ["country", "gender", "education", "income_5", "emp_2010", "agegroups4", "globalregion"]
    dict_q9 = {1: "Mostly help", 2: "Mostly Harm", 3: "Don't have an opinion", 4: "Neither", 98: "DK", 99: "Refused"}
    dict_q8 = {1: "Yes, would feel safe", 2: "No, would not feel safe", 98: "DK", 99: "Refused"}

    df_q8_list = []
    for column in columns_to_split_by:
        df_q8_list.append(question_8("q8", df, column, dict_q8))

    df_q8_c = pd.concat(df_q8_list)
    df_q8_c.reset_index(drop=True, inplace=True)

    df_q9_list = []
    for column in columns_to_split_by:
        df_q9_list.append(question_8("q9", df, column, dict_q9))

    df_q9_c = pd.concat(df_q9_list)
    df_q9_c.reset_index(drop=True, inplace=True)

    df_merge = pd.merge(df_q9_c, df_q8_c, on=columns_to_split_by + ["year"], how="outer")

    tb = Table(df_merge, short_name="ai_wrp_2021", underscore=True)
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
    Calculates the percentage of valid responses for a given column in a DataFrame.
    Args:
        df (DataFrame): The input DataFrame.
        column (str): The column name.
        valid_responses_dict (dict): A dictionary mapping valid response codes to their corresponding labels.
    Returns:
        DataFrame: A DataFrame with columns: "country", "year", "column", "count", and "percentage".
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


def question_8(q, df, column_to_split_by, dict_q):
    """
    Computes the ratio of responses indicating a great deal or very much worry about terrorist attacks
    to responses indicating not at all or not much worry.
    Args:
        df (DataFrame): The input DataFrame.
    Returns:
        DataFrame: A DataFrame with columns: "country", "year", and "great_deal_or_very_much_not_at_all_or_not_much_ratio".
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

    # Compute the ratio of great deal or very much to not at all or not much
    if q == "q9":
        pivoted_df["help_harm_ratio"] = pivoted_df["Mostly help"] / pivoted_df["Mostly Harm"]
        return pivoted_df[["year", column_to_split_by, "help_harm_ratio"]]

    else:
        pivoted_df["yes_no_ratio"] = pivoted_df["Yes, would feel safe"] / pivoted_df["No, would not feel safe"]
        return pivoted_df[["year", column_to_split_by, "yes_no_ratio"]]


def map_values(df):
    gender = {1: "Male", 2: "Female"}

    education_level = {
        1: "Primary (0-8 years)",
        2: "Secondary (9-15 years)",
        3: "Tertiary (16 years or more)",
        9: "DK/Refused",
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
