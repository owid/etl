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
    log.info("longitudinal_wvs.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("longitudinal_wvs"))

    # Read table from meadow dataset.
    tb = ds_meadow["longitudinal_wvs"]
    #
    # Process data.
    #
    log.info("longitudinal_wvs.harmonize_countries")
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    q1 = question_1(tb)
    q2 = question_2(tb)
    q3 = question_3(tb)
    merge_q1_q2 = pd.merge(q1, q2, on=["country", "year"], how="outer")
    merge_q1_q2_q3 = pd.merge(merge_q1_q2, q3, on=["country", "year"], how="outer")
    tb = Table(merge_q1_q2_q3, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("longitudinal_wvs.end")


def calculate_percentage(df, column, valid_responses_dict):
    valid_responses = df[column].isin(valid_responses_dict.keys())
    df_filtered = df[["country", "year", column]][valid_responses].reset_index(drop=True)
    grouped = df_filtered.groupby(["country", "year"])
    counts = grouped[column].value_counts().reset_index(name="count")
    total_counts = counts.groupby(["country", "year"])["count"].transform("sum")
    counts["percentage"] = (counts["count"] / total_counts) * 100
    counts[column] = counts[column].map(valid_responses_dict)
    return counts


def question_1(df):
    dict_q1 = {1: "Very much", 2: "A great deal", 3: "Not much", 4: "Not at all", -1: "Don ́t know", -2: "No answer"}
    counts_q1 = calculate_percentage(df, "worries__a_terrorist_attack", dict_q1)
    select_df = counts_q1[["country", "year", "percentage", "worries__a_terrorist_attack"]]
    pivoted_df = select_df.pivot(index=["country", "year"], columns="worries__a_terrorist_attack", values="percentage")
    pivoted_df.reset_index(inplace=True)
    pivoted_df.columns.name = None
    pivoted_df["great_deal_or_very_much"] = pivoted_df["A great deal"] + pivoted_df["Very much"]
    pivoted_df["not_at_all_or_not_much"] = pivoted_df["Not at all"] + pivoted_df["Not much"]
    pivoted_df["great_deal_or_very_much_not_at_all_or_not_much_ratio"] = (
        pivoted_df["great_deal_or_very_much"] / pivoted_df["not_at_all_or_not_much"]
    )
    return pivoted_df[["country", "year", "great_deal_or_very_much_not_at_all_or_not_much_ratio"]]


def question_2(df):
    dict_q2 = {0: "Disagree", 1: "Hard to say", 2: "Agree", -1: "Don ́t know", -2: "No answer"}
    counts_q2 = calculate_percentage(
        df, "effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism", dict_q2
    )
    select_df = counts_q2[
        [
            "country",
            "year",
            "percentage",
            "effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism",
        ]
    ]
    pivoted_df = select_df.pivot(
        index=["country", "year"],
        columns="effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism",
        values="percentage",
    )
    pivoted_df.reset_index(inplace=True)
    pivoted_df.columns.name = None
    pivoted_df["agree_disagree"] = pivoted_df["Agree"] / pivoted_df["Disagree"]
    return pivoted_df[["country", "year", "agree_disagree"]]


def question_3(df):
    # Quesiton 3
    valid_resp_q3 = list(range(11))
    valid_responses_q3 = df["justifiable__terrorism_as_a_political__ideological_or_religious_mean"].isin(valid_resp_q3)
    df_q3 = df[valid_responses_q3]
    grouped_q3 = df_q3.groupby(["country", "year"])
    df_q3 = (
        grouped_q3["justifiable__terrorism_as_a_political__ideological_or_religious_mean"]
        .mean()
        .reset_index(name="average_from_never_1_always_10")
    )
    return df_q3
