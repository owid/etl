"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import owid.catalog.processing as pr
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

    # Capture origins from a source column so we can propagate them through
    # downstream pivots/value_counts/pivot_table operations, which create
    # entirely new columns and drop column-level origins.
    source_origins = list(tb["q8"].metadata.origins)

    #
    # Process data.
    #
    log.info("wrp_2021.harmonize_countries")
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # List of column names to keep (q8 and q9 are AI related)
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

    # Filter to keep only the AI related columns
    tb = tb[select_cols]

    # Map numerical values to categorical for certain columns
    tb = map_values(tb)

    # List of columns to split by when calculating question responses
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

    # Build per-split tables for question 8 and concatenate
    tb_q8_list = [question_extract("q8", tb, column, dict_q8) for column in columns_to_split_by]
    tb_q8_c = pr.concat(tb_q8_list, ignore_index=True)

    # Build per-split tables for question 9 and concatenate
    tb_q9_list = [question_extract("q9", tb, column, dict_q9) for column in columns_to_split_by]
    tb_q9_c = pr.concat(tb_q9_list, ignore_index=True)

    # Merge the two combined tables on common columns
    tb_merge = pr.merge(tb_q9_c, tb_q8_c, on=columns_to_split_by + ["year"], how="outer")

    # Now split categories (gender, income etc) into separate columns
    # Copy tb without categories (gender, income etc)
    tb_without_categories = (
        tb_merge[
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

    merge_rest = calculate_world_data(tb_merge, tb_without_categories)

    tb = Table(merge_rest, short_name=paths.short_name, underscore=True)

    tb["dk_no_op"] = tb[["dk__help_harm", "dont_have_an_opinion"]].sum(axis=1).values
    tb["other_help_harm"] = tb[["dk__help_harm", "dont_have_an_opinion", "refused__help_harm"]].sum(axis=1).values
    tb["other_yes_no"] = tb[["dk__cars", "refused__cars"]].sum(axis=1).values
    tb[["dk_no_op", "other_help_harm", "other_yes_no"]] = tb[["dk_no_op", "other_help_harm", "other_yes_no"]].replace(
        0.0, np.nan
    )

    # Re-stamp origins onto every output column (pivots/value_counts dropped them).
    for col in tb.columns:
        tb[col].metadata.origins = list(source_origins)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_wrp_2021.end")


def calculate_percentage(tb, column, valid_responses_dict, column_to_split_by):
    """
    Calculates the percentage of valid responses for a given column in a table, split by another column.
    """
    # Filter out invalid responses
    valid_responses = tb[column].isin(valid_responses_dict.keys())
    tb_filtered = tb[[column_to_split_by, "year", column]][valid_responses].reset_index(drop=True)

    # Group by country and year
    grouped = tb_filtered.groupby([column_to_split_by, "year"], observed=True)

    # Count valid responses
    counts = grouped[column].value_counts().reset_index(name="count")

    # Calculate total counts for each country and year
    total_counts = counts.groupby([column_to_split_by, "year"])["count"].transform("sum")

    # Calculate percentage
    counts["percentage"] = (counts["count"] / total_counts) * 100
    # Map response codes to labels
    counts[column] = counts[column].map(valid_responses_dict)

    return counts


def question_extract(q, tb, column_to_split_by, dict_q):
    """
    Computes the share of responses for a given column in a table, split by another column.
    """
    counts_q = calculate_percentage(tb, q, dict_q, column_to_split_by)

    # Select relevant columns
    select_tb = counts_q[
        [
            column_to_split_by,
            "year",
            "percentage",
            q,
        ]
    ]

    # Pivot the table
    pivoted_tb = select_tb.pivot(
        index=[column_to_split_by, "year"],
        columns=q,
        values="percentage",
    )
    pivoted_tb = pivoted_tb.reset_index()
    pivoted_tb.columns.name = None

    if q == "q9":
        return pivoted_tb[
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
        return pivoted_tb[
            ["year", column_to_split_by, "Yes, would feel safe", "No, would not feel safe", "DK(cars)", "Refused(cars)"]
        ]


def calculate_world_data(tb_merge, tb_without_categories):
    # Select rows with categories (NaN country rows)
    world_tb = tb_merge[tb_merge["country"].isna()].copy()
    world_tb = world_tb.reset_index(drop=True)

    # Set country as World
    world_tb["country"] = world_tb["country"].astype(str)
    world_tb.loc[world_tb["country"] == "nan", "country"] = "World"

    # Calculate the percentage of valid responses for "Mostly help", "Mostly harm", "Neither" in a table,
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
        conc_tb = pivot_by_category(world_tb, column)
        if merge_help_harm_all is None:
            merge_help_harm_all = conc_tb
        else:
            merge_help_harm_all = pr.merge(merge_help_harm_all, conc_tb, on=["year", "country"], how="outer")

    # Calculate the percentage of valid responses for "Yes, would feel safe" in a table, split by gender, income etc.
    columns_to_calculate = ["Yes, would feel safe", "No, would not feel safe", "DK(cars)", "Refused(cars)"]
    merge_yes_no = None
    for column in columns_to_calculate:
        conc_tb = pivot_by_category(world_tb, column)
        if merge_yes_no is None:
            merge_yes_no = conc_tb
        else:
            merge_yes_no = pr.merge(merge_yes_no, conc_tb, on=["year", "country"], how="outer")

    # Merge all tables into one
    merge_categorized = pr.merge(merge_help_harm_all, merge_yes_no, on=["year", "country"], how="outer")
    merge_rest = pr.merge(tb_without_categories, merge_categorized, on=["year", "country"], how="outer")

    merge_rest = merge_rest.set_index(["year", "country"])
    return merge_rest


def map_values(tb):
    """
    Maps numerical values to categorical for certain columns in the table.
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

    tb["gender"] = tb["gender"].map(gender)
    tb["education"] = tb["education"].map(education_level)
    tb["income_5"] = tb["income_5"].map(wealth_quintile)
    tb["emp_2010"] = tb["emp_2010"].map(employment_status)
    tb["agegroups4"] = tb["agegroups4"].map(age_group)
    tb["globalregion"] = tb["globalregion"].map(region)

    return tb


def pivot_by_category(tb, question):
    """
    Pivot the input table by categories and a specific question.
    """
    pivot_tables = []
    cols_pivot = ["gender", "education", "income_5", "emp_2010", "agegroups4", "globalregion"]

    for pivot_col in cols_pivot:
        pivoted_tb = tb.pivot_table(values=question, index=["country", "year"], columns=pivot_col, observed=True)
        pivot_tables.append(pivoted_tb)

    # Concatenate all the pivot tables along the columns
    conc_tb = pr.concat(pivot_tables, axis=1)

    # Add suffix to column names
    conc_tb = conc_tb.add_suffix("_" + question)

    # Reset index so ["year", "country"] become columns for downstream merging.
    conc_tb = conc_tb.reset_index()

    return conc_tb
