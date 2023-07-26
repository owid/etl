"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def melt_and_clean(df: pd.DataFrame, col_name: str, excluded_columns: List[str]) -> pd.DataFrame:
    """
    Melt and clean dataframe based on column name.
    """
    melted_df = pd.melt(
        df.reset_index(),
        id_vars=["year", "country"],
        value_vars=[col for col in df.columns if col_name in col and col not in excluded_columns],
    )
    melted_df["group"] = (
        melted_df["variable"].str.split("_" + col_name, expand=True)[0].str.replace("_", " ").str.title()
    )
    melted_df.rename(columns={"value": f"{col_name}_value"}, inplace=True)
    melted_df = melted_df[melted_df[f"{col_name}_value"].notnull()]
    return melted_df[["year", f"{col_name}_value", "group"]]


def run(dest_dir: str) -> None:
    log.info("ai_wrp_2021_grouped.start")

    # Load meadow dataset.
    ds_garden = cast(Dataset, paths.load_dependency("ai_wrp_2021"))
    df = pd.DataFrame(ds_garden["ai_wrp_2021"])

    columns_to_melt = [
        "yes__would_feel_safe",
        "no__would_not_feel_safe",
        "dk__cars",
        "refused__cars",
        "mostly_help",
        "mostly_harm",
        "neither",
        "dk__help_harm",
        "dont_have_an_opinion",
        "refused__help_harm",
    ]

    # Define a common list of excluded columns.
    excluded_columns = [
        "yes__would_feel_safe",
        "mostly_help",
        "no__would_not_feel_safe",
        "mostly_harm",
        "other_yes_no",
        "other_help_harm",
        "neither",
        "refused__cars",
        "dk__cars",
        "refused__help_harm",
        "dk_no_op",
        "dk__help_harm",
        "dont_have_an_opinion",
    ]

    # Using a dictionary to store the melted dataframes.
    melted_dfs = {}

    for column in columns_to_melt:
        melted_dfs[column] = melt_and_clean(df, column, excluded_columns)

    merge_all = melted_dfs[columns_to_melt[0]]

    # Merge all melted dataframes together.
    for column in columns_to_melt[1:]:
        merge_all = pd.merge(merge_all, melted_dfs[column], on=["year", "group"], how="outer")

    # Derive additional columns (mainly to avoid grapher errors)
    merge_all["other_yes_no_value"] = merge_all["dk__cars_value"] + merge_all["refused__cars_value"]
    merge_all["other_help_harm_value"] = (
        merge_all["dk__help_harm_value"]
        + merge_all["dont_have_an_opinion_value"]
        + merge_all["refused__help_harm_value"]
    )
    merge_all["dk_no_op_value"] = merge_all["dk__help_harm_value"] + merge_all["dont_have_an_opinion_value"]

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

    merge_all["group"].replace(group_replacements, inplace=True)
    merge_all.set_index(["year", "group"], inplace=True)

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[Table(merge_all, short_name=paths.short_name, underscore=True)],
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_wrp_2021_grouped.end")
