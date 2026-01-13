"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def inspect_demographics(tb):
    """Inspect demographic distribution of survey respondents.

    This function creates visualizations of the demographic breakdown.
    Uncomment the call in run() to use it.
    """
    import pandas as pd
    import plotly.express as px

    # Get unique users to avoid counting same demographics multiple times.
    # The meadow table has user and question as regular columns, not in the index.
    # Convert to pandas DataFrame to avoid catalog metadata issues with division.
    tb_users = pd.DataFrame(
        tb[["user", "age", "gender", "political_views", "income", "race", "region", "state"]]
    ).drop_duplicates(subset=["user"])

    # Demographics indicators available in the U.S. survey.
    demographics_expected = [
        "gender",
        "political_views",
        "age",
        "income",
        "race",
        "region",
    ]

    # Create a dictionary of demographics indicators, and a table of percentages.
    demographics_found = {
        indicator: tb_users.groupby(indicator).size() / len(tb_users) * 100 for indicator in demographics_expected
    }

    # Loop through demographics and plot bar charts.
    for indicator in demographics_expected:
        fig = px.bar(
            demographics_found[indicator].reset_index().rename(columns={0: "percentage"}),
            x=indicator,
            y="percentage",
            labels={"percentage": "Percentage of respondents", indicator: ""},
            title=f"Distribution by {indicator.replace('_', ' ')}",
        )
        fig.show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("acceptability_of_us_farming_practices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("acceptability_of_us_farming_practices")

    #
    # Process data.
    #
    # Uncomment to inspect demographics.
    # inspect_demographics(tb=tb)

    # Create a counts table with responses per question.
    tb_counts = (
        tb.groupby(["question", "answer"], as_index=False)
        .agg({"user": "count"})
        .rename(columns={"user": ""})
        .pivot(index="question", columns="answer", join_column_levels_with="")
        .set_index("question")
    )
    # For some reason, indicators' metadata is not propagated, copy it from the original table.
    for column in tb_counts.columns:
        tb_counts[column] = tb_counts[column].copy_metadata(tb["answer"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_counts], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
