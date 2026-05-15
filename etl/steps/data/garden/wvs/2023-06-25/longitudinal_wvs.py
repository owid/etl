"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import owid.catalog.processing as pr
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
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    cols = [c for c in tb.columns if c not in ["country", "year"]]
    tb[cols] = tb[cols].astype(float)

    # Compute the ratio of responses indicating a great deal or very much worry about terrorist attacks
    q1 = question_1(tb)
    # Compute the ratio of agree to disagree responses regarding the effects of immigrants on the risks of terrorism.
    q2 = question_2(tb)
    # Compute the average response to the question on justifiability of terrorism as a political, ideological, or religious mean.
    q3 = question_3(tb)

    merge_q1_q2 = pr.merge(q1, q2, on=["country", "year"], how="outer")
    # Merge all 3 questions
    tb_out = pr.merge(merge_q1_q2, q3, on=["country", "year"], how="outer")
    tb_out.metadata.short_name = paths.short_name
    tb_out = tb_out.underscore()
    tb_out = tb_out.set_index(["country", "year"])
    tb_out = tb_out.dropna(how="all")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_out], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("longitudinal_wvs.end")


def calculate_percentage(tb: Table, column: str, valid_responses_dict: dict) -> Table:
    """
    Calculates the percentage of valid responses for a given column in a Table.

    Args:
        tb (Table): The input Table.
        column (str): The column name.
        valid_responses_dict (dict): A dictionary mapping valid response codes to their corresponding labels.

    Returns:
        Table: A Table with columns: "country", "year", "column", "count", and "percentage".
    """
    # Filter out invalid responses
    valid_responses = tb[column].isin(valid_responses_dict.keys())
    tb_filtered = tb[["country", "year", column]][valid_responses].reset_index(drop=True)

    # Group by country and year
    grouped = tb_filtered.groupby(["country", "year"])

    # Count valid responses
    counts = grouped[column].value_counts().reset_index(name="count")

    # Calculate total counts for each country and year
    total_counts = counts.groupby(["country", "year"])["count"].transform("sum")

    # Calculate percentage
    counts["percentage"] = (counts["count"] / total_counts) * 100

    # Map response codes to labels
    counts[column] = counts[column].map(valid_responses_dict)

    return counts


def question_1(tb: Table) -> Table:
    """
    Computes the ratio of responses indicating a great deal or very much worry about terrorist attacks
    to responses indicating not at all or not much worry.

    Args:
        tb (Table): The input Table.

    Returns:
        Table: A Table with columns: "country", "year", and "great_deal_or_very_much_not_at_all_or_not_much_ratio".
    """
    dict_q1 = {1: "Very much", 2: "A great deal", 3: "Not much", 4: "Not at all", -1: "Don't know", -2: "No answer"}

    # Calculate percentage for worries about a terrorist attack
    counts_q1 = calculate_percentage(tb, "worries__a_terrorist_attack", dict_q1)

    # Select relevant columns
    select_tb = counts_q1[["country", "year", "percentage", "worries__a_terrorist_attack"]]
    # Pivot the Table
    pivoted_tb = pr.pivot(
        select_tb, index=["country", "year"], columns="worries__a_terrorist_attack", values="percentage"
    )
    pivoted_tb = pivoted_tb.reset_index()
    pivoted_tb.columns.name = None

    # Compute the ratio of great deal or very much to not at all or not much
    pivoted_tb["great_deal_or_very_much"] = pivoted_tb["A great deal"] + pivoted_tb["Very much"]
    pivoted_tb["not_at_all_or_not_much"] = pivoted_tb["Not at all"] + pivoted_tb["Not much"]
    pivoted_tb["great_deal_or_very_much_not_at_all_or_not_much_ratio"] = (
        pivoted_tb["great_deal_or_very_much"] / pivoted_tb["not_at_all_or_not_much"]
    )
    pivoted_tb = pivoted_tb[
        [
            "country",
            "year",
            "great_deal_or_very_much",
            "not_at_all_or_not_much",
            "great_deal_or_very_much_not_at_all_or_not_much_ratio",
        ]
    ]

    return pivoted_tb


def question_2(tb: Table) -> Table:
    """
    Computes the ratio of agree to disagree responses regarding the effects of immigrants on the risks of terrorism.

    Args:
        tb (Table): The input Table.

    Returns:
        Table: A Table with columns: "country", "year", and "agree_disagree".
    """
    dict_q2 = {0: "Disagree", 1: "Hard to say", 2: "Agree", -1: "Don't know", -2: "No answer"}

    # Calculate percentage for effects of immigrants on the risks of terrorism
    counts_q2 = calculate_percentage(
        tb, "effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism", dict_q2
    )

    # Select relevant columns
    select_tb = counts_q2[
        [
            "country",
            "year",
            "percentage",
            "effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism",
        ]
    ]

    # Pivot the Table
    pivoted_tb = pr.pivot(
        select_tb,
        index=["country", "year"],
        columns="effects_of_immigrants_on_the_development_of__your_country__increase_the_risks_of_terrorism",
        values="percentage",
    )
    pivoted_tb = pivoted_tb.reset_index()
    pivoted_tb.columns.name = None

    # Compute the ratio of agree to disagree
    pivoted_tb["agree_disagree"] = pivoted_tb["Agree"] / pivoted_tb["Disagree"]

    return pivoted_tb[["country", "year", "Agree", "Disagree", "agree_disagree"]]


def question_3(tb: Table) -> Table:
    """
    Computes the average response to the question on justifiability of terrorism as a political, ideological,
    or religious mean.

    Args:
        tb (Table): The input Table.

    Returns:
        Table: A Table with columns: "country", "year", and "average_from_never_1_always_10".
    """
    valid_resp_q3 = list(range(11))

    # Filter out invalid responses
    valid_responses_q3 = tb["justifiable__terrorism_as_a_political__ideological_or_religious_mean"].isin(valid_resp_q3)
    tb_q3 = tb[valid_responses_q3]

    # Group by country and year, and calculate the average response
    grouped_q3 = tb_q3.groupby(["country", "year"])
    tb_q3 = (
        grouped_q3["justifiable__terrorism_as_a_political__ideological_or_religious_mean"]
        .mean()
        .reset_index(name="average_from_never_1_always_10")
    )

    return tb_q3
