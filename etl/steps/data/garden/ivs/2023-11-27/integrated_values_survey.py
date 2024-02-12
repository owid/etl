"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("integrated_values_survey")

    # Read table from meadow dataset.
    tb = ds_meadow["integrated_values_survey"].reset_index()

    #
    # Process data.

    # Drop columns
    tb = drop_indicators_and_replace_nans(tb)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_indicators_and_replace_nans(tb: Table) -> Table:
    """
    Drop indicators/questions not useful enough for OWID's purposes (too few data points, for too few countries, etc.)
    Also, replace zero values appearing in IVS data with nulls. This did not happen in WVS data.
    """

    # Drop selected variables
    vars_to_drop = [
        "trust_first_not_very_much",
        "trust_personally_not_very_much",
        "confidence_confidence_in_cer_with_australia",  # Only New Zealand
        "confidence_free_commerce_treaty__tratado_de_libre_comercio",  # Only Mexico and Chile
        "confidence_united_american_states_organization",  # Only Peru and Dominican Republic
        "confidence_education_system",  # most of the data is in 1993
        "confidence_social_security_system",  # most of the data is in 1993
        "confidence_andean_pact",  # Only Venezuela
        "confidence_local_regional_government",  # Only Argentina and Puerto Rico
        "confidence_organization_of_american_states__oae",  # Only Peru
    ]
    tb = tb.drop(columns=vars_to_drop)

    # Define columns containing "missing" and "dont_know"
    missing_cols = [cols for cols in tb.columns if "missing" in cols]
    dont_know_cols = [cols for cols in tb.columns if "dont_know" in cols]

    # Replace zero values with nulls, except for columns containing "missing" and "dont_know"
    subset_cols = tb.columns.difference(missing_cols + dont_know_cols)
    tb[subset_cols] = tb[subset_cols].replace(0, float("nan"))

    # Replace 100 by null in columns containing "missing"
    tb[missing_cols] = tb[missing_cols].replace(100, float("nan"))

    # Replace 0 by null for don't know columns if the rest of columns are null
    # For important in life questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=[
            "important_in_life_family",
            "important_in_life_friends",
            "important_in_life_leisure_time",
            "important_in_life_politics",
            "important_in_life_work",
            "important_in_life_religion",
        ],
        answers=[
            "very",
            "rather",
            "not_very",
            "notatall",
        ],
    )

    # For interested in politics question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["interested_politics"],
        answers=["very", "somewhat", "not_very", "not_at_all"],
    )

    # For political action questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=[
            "political_action_signing_a_petition",
            "political_action_joining_in_boycotts",
            "political_action_attending_peaceful_demonstrations",
            "political_action_joining_unofficial_strikes",
        ],
        answers=["have_done", "might_do", "never"],
    )

    # For environment vs. economy question
    tb = replace_dont_know_by_null(tb=tb, questions=["env_ec"], answers=["environment", "economy", "other_answer"])

    # For income equality question
    tb = replace_dont_know_by_null(tb=tb, questions=["eq_ineq"], answers=["equality", "neutral", "inequality"])

    # Drop rows with all null values in columns not country and year
    tb = tb.dropna(how="all", subset=tb.columns.difference(["country", "year"]))

    return tb


def replace_dont_know_by_null(tb: Table, questions: list, answers: list) -> Table:
    """
    Replace empty don't know answers when the rest of the answers is null
    """
    for q in questions:
        # Add q to each member of answers
        answers_by_question = [f"{a}_{q}" for a in answers]

        # Check if all columns in answers_by_question are null
        tb["null_check"] = tb[answers_by_question].notnull().all(axis=1)

        print(
            tb[(tb["country"] == "Great Britain") & (tb["year"] >= 1984)][
                ["country", "year"] + answers_by_question + [f"dont_know_{q}", "null_check"]
            ]
        )

        # if null_check is False and f"dont_know_{q}" is 0, set f"dont_know_{q}" as null
        tb.loc[(~tb["null_check"]) & (tb[f"dont_know_{q}"] == 0), f"dont_know_{q}"] = float("nan")

    # Remove null_check
    tb = tb.drop(columns=["null_check"])

    return tb
