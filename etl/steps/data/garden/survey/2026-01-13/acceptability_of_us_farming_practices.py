"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define mappings from raw column names to descriptive question text.
# Based on the Faunalytics survey on standard U.S. animal agriculture practices.

# Columns on people's acceptability of animal welfare practices.
# The questions are organized by animal type (pigs, laying hens, cows, broilers).
# Question text taken from the survey instrument as shown in the Faunalytics report.
COLUMNS_ACCEPTABILITY = {
    # Pig practices.
    "pigs_1": "Cutting the tails off new-born piglets, almost always without anesthesia or pain-killers",
    "pigs_2": "Killing pigs in gas chambers by use of carbon dioxide (CO2) gas",
    "pigs_3": "Keeping pigs in cages which prevent them from turning around for several weeks",
    # Laying hen practices.
    "layinghens_1": "Keeping chickens in cages with 67-86 square inches of space per bird (smaller than a standard sheet of letter paper)",
    "layinghens_2": "Cutting the beaks off new-born chickens, almost always without anesthesia or pain-killers",
    "layinghens_3": "Killing new-born male chicks who can't lay eggs by use of meat-grinders",
    # Cow/cattle practices.
    "cows_1": "Castrating new-born calves by surgically removing the testicles, almost always without anesthesia or pain-killers",
    "cows_2": "Removing calves' horn buds (undeveloped horn tissue before it grows into a visible horn) using a knife or hot iron, sometimes without anesthesia or pain-killers",
    "cows_3": "Permanently separating calves from their mothers immediately after birth",
    # Broiler chicken practices.
    "broilers_1": "Keeping chickens inside a barn with no outdoor access and with less than one square foot of space per bird",
    "broilers_2": "Growing chickens to reach market weight size by 47 days, leading to difficulties with walking and standing",
    "broilers_3": "Hanging live chickens upside down by their legs before stunning them in electrified water, slitting their throats, and finally submerging them in boiling water",
}

# Demographic columns mapping.
# Use the "pooled" versions which have cleaned and standardized values.
COLUMNS_DEMOGRAPHICS = {
    "age": "age",
    "gender_pooled": "gender",
    "income": "income",
    "political_pooled": "political_views",
    "state": "state",
    "region": "region",
    "race_groups": "race",
}

# Other auxiliary columns.
COLUMNS_OTHER = {
    "response_id": "response_id",
    "duration_secs": "duration_seconds",
    "status": "status",
    "weight": "weight",  # Census-based survey weights for representative analysis.
}

# All columns to keep.
COLUMNS = {**COLUMNS_ACCEPTABILITY, **COLUMNS_DEMOGRAPHICS, **COLUMNS_OTHER}

# Map acceptability responses to standardized values.
# The survey uses a 5-point scale from "Very unacceptable" to "Very acceptable".
ACCEPTABILITY = {
    "Very unacceptable": "Very unacceptable",
    "Somewhat unacceptable": "Somewhat unacceptable",
    # Fix grammar.
    "Neither acceptable or unacceptable": "Neither acceptable nor unacceptable",
    "Somewhat acceptable": "Somewhat acceptable",
    "Very acceptable": "Very acceptable",
}


def sanity_check_inputs(tb):
    # Check that we don't have any missing data in core question columns.
    question_cols = [col for col in COLUMNS_ACCEPTABILITY.keys() if col in tb.columns]
    error = "Found missing values in question columns."
    for col in question_cols:
        assert tb[col].notna().all(), f"{error} Column {col} has {tb[col].isna().sum()} missing values."

    # Check that status is "Complete" for all responses.
    error = "Found incomplete survey responses."
    assert tb["status"].eq("Complete").all(), error

    # Check that response IDs are unique.
    error = "Found duplicate response IDs."
    assert not tb["response_id"].duplicated().any(), error

    # Verify expected response values.
    expected_values = {
        "Very unacceptable",
        "Somewhat unacceptable",
        "Neither acceptable or unacceptable",
        "Somewhat acceptable",
        "Very acceptable",
    }
    for col in question_cols:
        actual_values = set(tb[col].dropna().unique())
        error = f"Unexpected response values in {col}."
        assert actual_values.issubset(expected_values), f"{error} Found: {actual_values - expected_values}"

    # Sanity checks on census-based survey weights.
    # Weights should sum to the number of respondents and have a mean of 1.0.
    error = "Survey weights have unexpected properties."
    assert tb["weight"].notna().all(), f"{error} Found missing weight values."
    weight_sum = tb["weight"].sum()
    assert abs(weight_sum - len(tb)) < 0.01, f"{error} Weights sum to {weight_sum:.2f}, expected {len(tb)}."
    weight_mean = tb["weight"].mean()
    assert abs(weight_mean - 1.0) < 0.01, f"{error} Weight mean is {weight_mean:.4f}, expected 1.0."
    # Verify weights are within reasonable bounds (typically between 0.1 and 10 for census weights).
    assert tb["weight"].min() > 0.1, f"{error} Minimum weight is {tb['weight'].min():.4f}, expected > 0.1."
    assert tb["weight"].max() < 10.0, f"{error} Maximum weight is {tb['weight'].max():.4f}, expected < 10.0."

    # Check that all responses took a minimum amount of time (otherwise, drop the ones that don't).
    assert tb[tb["duration_seconds"] < 60].empty


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
        tb[["response_id", "age", "gender", "political_views", "income", "race", "region", "state"]]
    ).drop_duplicates(subset=["response_id"])

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


def sanity_check_outputs(tb):
    error = "Unexpectedly low number of valid survey responses."
    assert len(tb) > 900, f"{error} Only {len(tb)} responses remain."


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
    # Select and rename columns.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Sanity checks on raw data.
    sanity_check_inputs(tb=tb)

    # Rename responses (to fix a small grammar mistake).
    for column in COLUMNS_ACCEPTABILITY.values():
        tb[column] = map_series(series=tb[column], mapping=ACCEPTABILITY)

    # Uncomment to inspect demographics.
    # inspect_demographics(tb=tb)

    # Sanity check outputs (on the long-format table).
    sanity_check_outputs(tb=tb)

    # Reshape to long format: create a row for each user-question combination.
    tb = tb.melt(
        id_vars=["response_id", "weight"] + list(COLUMNS_DEMOGRAPHICS.values()),
        value_vars=list(COLUMNS_ACCEPTABILITY.values()),
        var_name="question",
        value_name="answer",
    )

    # Create counts table with responses per question.
    tb_counts = (
        tb.groupby(["question", "answer"], as_index=False)
        .agg({"response_id": "count", "weight": "sum"})
        .rename(columns={"response_id": "counts", "weight": "counts_weighted"}, errors="raise")
    )
    tb_counts = tb_counts.pivot(index="question", columns="answer", join_column_levels_with="_")

    # Improve table formats.
    tb_counts = tb_counts.format(["question"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_counts])

    # Save garden dataset.
    ds_garden.save()
