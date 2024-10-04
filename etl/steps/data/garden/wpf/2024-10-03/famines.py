"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("famines")

    # Read table from meadow dataset.
    tb = ds_meadow["famines"].reset_index()

    #
    # Process data.
    #
    tb = process_causes(tb)

    # Divide each row's 'wpf_authoritative_mortality_estimate' by the length of the corresponding 'Date' value to assume a uniform distribution of deaths over the period
    tb["wpf_authoritative_mortality_estimate"] = tb.apply(
        lambda row: row["wpf_authoritative_mortality_estimate"] / len(row["date"])
        if pd.notna(row["date"])
        else row["wpf_authoritative_mortality_estimate"],
        axis=1,
    )
    # Convert 'wpf_authoritative_mortality_estimate' to integer
    tb["wpf_authoritative_mortality_estimate"] = (
        pd.to_numeric(tb["wpf_authoritative_mortality_estimate"], errors="coerce").fillna(0).astype(int)
    )

    # Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    tb = unravel_dates(tb)

    # unique_titles = tb["conventional_title"].unique()
    # for title in unique_titles:
    #    print(title)

    for col in ["wpf_authoritative_mortality_estimate", "conflict", "government_policy_overall", "external_factors"]:
        tb[col].metadata.origins = tb["conventional_title"].metadata.origins
    tb = tb.format(["conventional_title", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_causes(tb):
    """
    Process the cause columns to create new columns for Conflict, Government policy overall, and External factors.
    """
    # Define keywords for each new column
    conflict_keywords = ["war", "blockade", "counterinsurgency", "occupation", "siege", "forced starvation", "genocide"]
    government_policy_keywords = [
        "economic policy",
        "government policy",
        "colonial policy",
        "state capacity",
        "taxation",
        "forced labor",
    ]
    external_factors_keywords = ["climate", "environment", "disease"]

    # Function to check if any keyword is in the text
    def contains_keywords(text, keywords):
        if pd.isna(text):
            return 0
        return int(any(keyword in text.lower() for keyword in keywords))

    # Apply the function to each row for each new column
    tb["conflict"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], conflict_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )
    tb["government_policy_overall"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], government_policy_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )
    tb["external_factors"] = tb.apply(
        lambda row: int(
            any(
                contains_keywords(row[col], external_factors_keywords)
                for col in ["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"]
            )
        ),
        axis=1,
    )

    tb = tb.drop(columns=["cause__immediate_trigger", "cause__contributing_factors", "cause__structural_factors"])

    return tb


def unravel_dates(tb):
    """
    Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    """
    # Split the 'date' column into multiple rows
    tb = tb.assign(date=tb["date"].str.split(",")).explode("date").drop_duplicates().reset_index(drop=True)

    return tb
