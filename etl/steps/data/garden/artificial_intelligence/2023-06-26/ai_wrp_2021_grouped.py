"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Function to melt and clean dataframe based on column name
def melt_and_clean(df, col_name):
    excluded_columns = [
        "yes__would_feel_safe",
        "mostly_help",
        "no__would_not_feel_safe",
        "mostly_harm",
        "other_help_harm",
        "other_yes_no",
        "neither",
    ]

    melted_df = pd.melt(
        df.reset_index(),
        id_vars=["year", "country"],
        value_vars=[col for col in df.columns if col_name in col and col not in excluded_columns],
    )
    melted_df[col_name] = (
        melted_df["variable"].str.split("_" + col_name, expand=True)[0].str.replace("_", " ").str.title()
    )
    melted_df.rename(columns={"value": f"{col_name}_value"}, inplace=True)
    melted_df.rename(columns={col_name: "group"}, inplace=True)

    melted_df = melted_df[melted_df[f"{col_name}_value"].notnull()]
    melted_df.reset_index(drop=True, inplace=True)

    return melted_df[["year", f"{col_name}_value", "group"]]


def run(dest_dir: str) -> None:
    log.info("ai_wrp_2021_grouped.start")

    # Load meadow dataset.
    ds_garden = cast(Dataset, paths.load_dependency("ai_wrp_2021"))

    # Read table from meadow dataset.
    df = pd.DataFrame(ds_garden["ai_wrp_2021"])

    # Melt and clean dataframes
    melted_yes = melt_and_clean(df, "yes__would_feel_safe").dropna(subset=["yes__would_feel_safe_value"])
    melted_no = melt_and_clean(df, "no__would_not_feel_safe").dropna(subset=["no__would_not_feel_safe_value"])
    merge_yes_no = pd.merge(melted_yes, melted_no, on=["year", "group"], how="outer")
    melted_help = melt_and_clean(df, "mostly_help").dropna(subset=["mostly_help_value"])
    melted_harm = melt_and_clean(df, "mostly_harm").dropna(subset=["mostly_harm_value"])
    melted_neither = melt_and_clean(df, "neither").dropna(subset=["neither_value"])
    merge_help_harm = pd.merge(melted_help, melted_harm, on=["year", "group"], how="outer")
    merge_help_harm_neither = pd.merge(merge_help_harm, melted_neither, on=["year", "group"], how="outer")
    merge_all = pd.merge(merge_yes_no, merge_help_harm_neither, on=["year", "group"], how="outer")

    merge_all["other_help_harm"] = 100 - (
        merge_all["mostly_help_value"] + merge_all["mostly_harm_value"] + merge_all["neither_value"]
    )

    merge_all["other_yes_no"] = 100 - (
        merge_all["yes__would_feel_safe_value"] + merge_all["no__would_not_feel_safe_value"]
    )

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
