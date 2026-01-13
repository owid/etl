"""Load a snapshot and create a meadow dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum number of seconds spent in a survey (shorter sessions will be removed).
# Based on the distribution, filtering out very fast responses that are likely not meaningful.
MINIMUM_SECONDS = 70

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
}

# All columns to keep.
COLUMNS = {**COLUMNS_ACCEPTABILITY, **COLUMNS_DEMOGRAPHICS, **COLUMNS_OTHER}


def sanity_check_inputs(tb):
    # Check that we don't have unexpected missing data in core question columns.
    question_cols = [col for col in COLUMNS_ACCEPTABILITY.keys() if col in tb.columns]
    error = "Unexpected number of missing values in question columns."
    for col in question_cols:
        missing_pct = tb[col].isna().sum() / len(tb) * 100
        assert missing_pct < 5, f"{error} Column {col} has {missing_pct:.1f}% missing values."

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


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("acceptability_of_us_farming_practices.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Select relevant columns (all columns that are in COLUMNS dictionary).
    columns_to_keep = [col for col in tb.columns if col in COLUMNS.keys()]
    tb = tb[columns_to_keep].copy()

    # Rename columns according to mapping.
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Sanity checks on raw data.
    sanity_check_inputs(tb=tb)

    # Filter out responses that were too fast (likely not meaningful).
    tb = tb[tb["duration_seconds"] >= MINIMUM_SECONDS].reset_index(drop=True)

    # Map acceptability responses to standardized values.
    # The survey uses a 5-point scale from "Very unacceptable" to "Very acceptable".
    acceptability_mapping = {
        "Very unacceptable": "Very unacceptable",
        "Somewhat unacceptable": "Somewhat unacceptable",
        "Neither acceptable or unacceptable": "Neither acceptable nor unacceptable",  # Fix grammar.
        "Somewhat acceptable": "Somewhat acceptable",
        "Very acceptable": "Very acceptable",
    }

    for column in COLUMNS_ACCEPTABILITY.values():
        tb[column] = map_series(
            series=tb[column],
            mapping=acceptability_mapping,
        )

    # No additional cleaning needed for demographics since we're using the pooled columns, which are already cleaned and standardized.

    # Keep only complete responses (all acceptability questions answered).
    acceptability_cols = list(COLUMNS_ACCEPTABILITY.values())
    tb = tb.dropna(subset=acceptability_cols, how="any").reset_index(drop=True)

    # Sanity check on final count.
    error = "Unexpectedly low number of valid survey responses."
    assert len(tb) > 900, f"{error} Only {len(tb)} responses remain."

    # Drop auxiliary columns we don't need in the final dataset.
    tb = tb.drop(columns=["response_id", "duration_seconds", "status"], errors="raise")

    # Reshape data: create a row for each user-question combination.
    # This makes it easier to analyze responses by question and demographic groups.
    demographic_cols = list(COLUMNS_DEMOGRAPHICS.values())
    tb = (
        tb.reset_index()
        .rename(columns={"index": "user"})
        .melt(id_vars=["user"] + demographic_cols, var_name="question", value_name="answer")
    )

    # Format table with appropriate index and data types.
    tb = tb.format(["user", "question"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
