"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Function to melt and clean dataframe based on column name
def melt_and_clean(df, col_name):
    excluded_columns = ["yes__would_feel_safe", "mostly_help", "no__would_not_feel_safe", "mostly_harm"]

    melted_df = pd.melt(
        df.reset_index(),
        id_vars=["year", "country"],
        value_vars=[col for col in df.columns if col_name in col and col not in excluded_columns],
    )
    melted_df[col_name] = (
        melted_df["variable"].str.split("_" + col_name, expand=True)[0].str.replace("_", " ").str.title()
    )
    melted_df.rename(columns={"value": f"{col_name}_value"}, inplace=True)
    melted_df = melted_df[melted_df[col_name] != "Dk Refused"]

    return melted_df[["year", f"{col_name}_value", col_name]]


# Adjusted function to assign indicator
def assign_indicator(df, column_name):
    conditions = []
    categories = []

    for method, mapping in condition_category_mapping.items():
        for category, values in mapping.items():
            if method == "isin":
                conditions.append(df[column_name].isin(values))
            elif method == "contains":
                conditions.append(df[column_name].str.contains("|".join(values)))
            categories.append(category)

    df["indicator"] = np.select(conditions, categories, default="Other")
    return df


# Adjusted dictionary of conditions and categories
condition_category_mapping = {
    "isin": {
        "Gender": ["Female", "Male"],
        "Education": ["Primary  0 8 Years ", "Secondary  9 15 Years ", "Tertiary  16 Years Or More "],
        "Income": ["Fourth 20Pct", "Middle 20Pct", "Poorest 20Pct", "Richest 20Pct", "Second 20Pct"],
        "Employment": [
            "Employed Full Time For An Employer",
            "Employed Full Time For Self",
            "Employed Part Time Do Not Want Full Time",
            "Employed Part Time Want Full Time",
            "Out Of Workforce",
            "Unemployed",
        ],
        "Age": [" 15 29", " 30 49", " 50 64", " 65Plus"],
    },
    "contains": {
        "Region": [
            "Australia And New Zealand",
            "Central",
            "East Asia",
            "Eastern",
            "Latin America  And  Caribbean",
            "Middle East",
            "North Africa",
            "Northern America",
            "Northern Western Europe",
            "South Asia",
            "South Eastern Asia",
            "Southern Africa",
            "Southern Europe",
        ]
    },
}


def reshape_dataframe(df, col_categories, col_values):
    reshaped_df = df.pivot(index=["year", col_categories], columns="indicator", values=col_values)
    reshaped_df.reset_index(inplace=True)
    reshaped_df.rename(columns={col_categories: "group"}, inplace=True)
    columns_to_rename = ["Age", "Education", "Employment", "Gender", "Income", "Region"]
    new_column_names = [col + " " + col_categories for col in columns_to_rename]

    rename_dict = dict(zip(columns_to_rename, new_column_names))

    reshaped_df.rename(columns=rename_dict, inplace=True)

    return reshaped_df


def run(dest_dir: str) -> None:
    log.info("ai_wrp_2021_grouped.start")

    # Load meadow dataset.
    ds_garden = cast(Dataset, paths.load_dependency("ai_wrp_2021"))

    # Read table from meadow dataset.
    df = pd.DataFrame(ds_garden["ai_wrp_2021"])

    # Melt and clean dataframes
    melted_yes = melt_and_clean(df, "yes__would_feel_safe").dropna(subset=["yes__would_feel_safe_value"])
    melted_no = melt_and_clean(df, "no__would_not_feel_safe").dropna(subset=["no__would_not_feel_safe_value"])
    melted_help = melt_and_clean(df, "mostly_help").dropna(subset=["mostly_help_value"])
    melted_harm = melt_and_clean(df, "mostly_harm").dropna(subset=["mostly_harm_value"])

    # Assign indicators
    melted_yes = assign_indicator(melted_yes, "yes__would_feel_safe")
    melted_no = assign_indicator(melted_no, "no__would_not_feel_safe")
    melted_help = assign_indicator(melted_help, "mostly_help")
    melted_harm = assign_indicator(melted_harm, "mostly_harm")

    # Reshape dataframes
    df_pivot_yes = reshape_dataframe(melted_yes, "yes__would_feel_safe", "yes__would_feel_safe_value")
    df_pivot_no = reshape_dataframe(melted_no, "no__would_not_feel_safe", "no__would_not_feel_safe_value")
    df_pivo_help = reshape_dataframe(melted_help, "mostly_help", "mostly_help_value")
    df_pivot_harm = reshape_dataframe(melted_harm, "mostly_harm", "mostly_harm_value")

    # Merge dataframes
    merged_help_harm = pd.merge(df_pivo_help, df_pivot_harm, on=["year", "group"], how="outer")
    merged_yes_no = pd.merge(df_pivot_yes, df_pivot_no, on=["year", "group"], how="outer")
    merged_groups = pd.merge(merged_help_harm, merged_yes_no, on=["year", "group"], how="outer")

    # Rename group values
    group_replacements = {
        " 15 29": "15-29 years",
        " 30 49": "30-49 years",
        " 50 64": "50-64 years",
        " 65Plus": "65+ years",
        "Australia And New Zealand": "Australia and New Zealand",
        "Fourth 20Pct": "Fourth 20%",
        "Middle 20Pct": "Middle 20%",
        "Poorest 20Pct": "Poorest 20%",
        "Richest 20Pct": "Richest 20%",
        "Second 20Pct": "Second 20%",
        "Primary  0 8 Years ": "Primary",
        "Secondary  9 15 Years ": "Secondary",
        "Tertiary  16 Years Or More ": "Tertiary",
        "Latin America And Caribbean": "Latin America and Caribbean",
        "Employed Full Time For An Employer": "Employed Full-Time (Employer)",
        "Employed Full Time For Self": "Employed Full-Time (Self)",
        "Employed Part Time Do Not Want Full Time": "Employed Part-Time (Not seeking Full-Time)",
        "Employed Part Time Want Full Time": "Employed Part-Time (Seeking Full-Time)",
    }
    merged_groups["group"].replace(group_replacements, inplace=True)
    merged_groups.set_index(["year", "group"], inplace=True)

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[Table(merged_groups, short_name=paths.short_name, underscore=True)],
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_wrp_2021_grouped.end")
