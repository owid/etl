"""Load a meadow dataset and create a garden dataset.

Minor cleaning of GHO dataset (only age-standardized suicide rates metrics)"""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    ds_gho = paths.load_dataset("gho")
    tb = ds_gho["age_standardized_suicide_rates__per_100_000_population"]

    # Rename and select subset of data
    cols = {"age_standardized_suicide_rates__per_100_000_population": "suicide_rate"}
    tb = tb.loc[:, cols.keys()].rename(columns=cols)

    #
    # Process data.
    #
    tb_ratio = process_ratio(tb)
    tb.m.short_name = "gho_suicides"
    tb_ratio.m.short_name = "gho_suicides_ratio"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_ratio], check_variables_metadata=True, default_metadata=ds_gho.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_ratio(df: Table) -> Table:
    df = df.reset_index().copy()
    # Get only male and female data separately
    df_male = df[df["sex"] == "male"].drop(columns=["sex"])
    df_female = df[df["sex"] == "female"].drop(columns=["sex"])
    # Merge data by year and country
    df_ratio = df_male.merge(df_female, on=["country", "year"], suffixes=("_m", "_f"))
    # Estimate ratio
    df_ratio["suicide_rate_male_to_female"] = df_ratio["suicide_rate_m"] / df_ratio["suicide_rate_f"]
    # Keep only relevant columns
    df_ratio = df_ratio[["country", "year", "suicide_rate_male_to_female"]]
    # Remove NaNs and infinities
    df_ratio = df_ratio.dropna(subset=["suicide_rate_male_to_female"])
    df_ratio = df_ratio[~df_ratio["suicide_rate_male_to_female"].isin([np.inf, -np.inf])]
    # Set index
    df_ratio = df_ratio.set_index(["country", "year"])
    return df_ratio


# def process_suicide_rates(df: Table) -> list[Table]:
#     # TODO: move this to its own dataset that depends on GHO
#     # TODO: work on index
#     df = df.rename(columns={"numeric": "suicide_rate"})

#     # Get only male and female data separately
#     df_male = df[df["sex"] == "male"].drop(columns=["sex"])
#     df_female = df[df["sex"] == "female"].drop(columns=["sex"])
#     # Merge data by year and country
#     df_ratio = df_male.merge(df_female, on=["country", "year"], suffixes=("_m", "_f"))
#     # Estimate ratio
#     df_ratio["suicide_rate_male_to_female"] = df_ratio["suicide_rate_m"] / df_ratio["suicide_rate_f"]
#     # Keep only relevant columns
#     df_ratio = df_ratio[["country", "year", "suicide_rate_male_to_female"]]
#     df = df.loc[:, ["country", "year", "sex", "suicide_rate"]]
#     # Remove NaNs and infinities
#     df_ratio = df_ratio.dropna(subset=["suicide_rate_male_to_female"])
#     df_ratio = df_ratio[~df_ratio["suicide_rate_male_to_female"].isin([np.inf, -np.inf])]
#     # Update metadata
#     df.m.short_name = "gho_suicides"
#     df_ratio.m.short_name = "gho_suicides_ratio"
#     return [df, df_ratio]  # type: ignore
