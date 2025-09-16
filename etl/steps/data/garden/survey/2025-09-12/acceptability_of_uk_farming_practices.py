"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def inspect_demographics(tb):
    import plotly.express as px

    tb = tb.copy()

    # For convenience, remove spurious symbols and explanations in labels.
    tb["education"] = tb["education"].str.replace("â€™", "'")
    tb["diet"] = tb["diet"].str.split(" (", regex=False).str[0]

    # Create a dictionary of demographics indicators, and a table of percentages.
    demographics_expected = [
        "gender",
        "political_views",
        "age_group",
        "town_size",
        "income",
        "diet",
        "education",
    ]
    # Create age groups.
    assert tb["age"].min() >= 18, "Unexpected age range."
    bins = [17, 24, 34, 44, 54, 64, 100]
    labels = [f"{bins[i]+1}-{bins[i+1]}" for i in range(len(bins) - 2)] + [f"{bins[-2]+1}+"]
    tb["age_group"] = pd.cut(tb["age"], bins=bins, labels=labels, include_lowest=True)
    demographics_found = {
        indicator: tb.groupby(indicator).agg({"user": lambda x: 100 * len(x) / len(tb)})
        for indicator in demographics_expected
    }
    demographics_found["age_group"] = tb.groupby("age_group").agg({"user": lambda x: 100 * len(x) / len(tb)})

    # Loop through demographics and plot bar charts.
    for indicator in demographics_expected:
        fig = px.bar(
            demographics_found[indicator].reset_index(),
            x=indicator,
            y="user",
            labels={"user": "Percentage of respondents", indicator: ""},
            title=f"Distribution by {indicator.replace('_', ' ')}",
        )
        fig.show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("acceptability_of_uk_farming_practices")

    # Read table from meadow dataset.
    tb = ds_meadow.read("acceptability_of_uk_farming_practices")

    #
    # Process data.
    #
    # Uncomment to inspect demographics.
    # inspect_demographics(tb=tb)

    # Rename columns.
    # For now, simply keep the total percentages, which is what we may use in a chart.
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
        tb_counts[column].metadata.unit = ""
        tb_counts[column].metadata.short_unit = ""

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_counts], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
