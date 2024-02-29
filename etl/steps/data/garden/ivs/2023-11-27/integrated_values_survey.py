"""Load a meadow dataset and create a garden dataset."""

from typing import List

from owid.catalog import Table
from structlog import get_logger
from tabulate import tabulate

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()

# Set table format when printing
TABLEFMT = "pretty"

# Set margin for checks
MARGIN = 0.5

# Define question suffixes


IMPORTANT_IN_LIFE_QUESTIONS = [
    "important_in_life_family",
    "important_in_life_friends",
    "important_in_life_leisure_time",
    "important_in_life_politics",
    "important_in_life_work",
    "important_in_life_religion",
]

INTERESTED_IN_POLITICS_QUESTIONS = ["interested_politics"]

POLITICAL_ACTION_QUESTIONS = [
    "political_action_signing_a_petition",
    "political_action_joining_in_boycotts",
    "political_action_attending_peaceful_demonstrations",
    "political_action_joining_unofficial_strikes",
]

ENVIRONMENT_VS_ECONOMY_QUESTIONS = ["env_ec"]

INCOME_EQUALITY_QUESTIONS = ["eq_ineq"]

SCHWARTZ_QUESTIONS = [
    "new_ideas",
    "rich",
    "secure",
    "good_time",
    "help_others",
    "success",
    "risks",
    "behave",
    "respect_environment",
    "tradition",
]

WORK_VS_LEISURE_QUESTIONS = ["lei_vs_wk"]

WORK_QUESTIONS = ["work_is_a_duty", "work_should_come_first"]

MOST_SERIOUS_PROBLEM_QUESTIONS = ["most_serious"]

JUSTIFIABLE_QUESTIONS = [
    "claiming_benefits",
    "stealing_property",
    "parents_beating_children",
    "violence_against_other_people",
    "avoiding_fare_on_public_transport",
    "cheating_on_taxes",
    "accepting_a_bribe",
    "homosexuality",
    "prostitution",
    "abortion",
    "divorce",
    "euthanasia",
    "suicide",
    "having_casual_sex",
    "sex_before_marriage",
    "invitro_fertilization",
    "death_penalty",
    "man_beating_wife",
    "political_violence",
]

WORRIES_QUESTIONS = ["losing_job", "not_being_able_to_provide_good_education", "war", "terrorist_attack", "civil_war"]

HAPPINESS_QUESTIONS = ["happy"]


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

    # Sanity checks
    tb = sanity_checks(tb)

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

    # Define columns containing "no_answer" and "dont_know"
    no_answer_cols = [cols for cols in tb.columns if "no_answer" in cols]
    dont_know_cols = [cols for cols in tb.columns if "dont_know" in cols]

    # Replace zero values with nulls, except for columns containing "no_answer" and "dont_know"
    subset_cols = tb.columns.difference(no_answer_cols + dont_know_cols)
    tb[subset_cols] = tb[subset_cols].replace(0, float("nan"))

    # Replace nulls in Schwartz questions by 0 when the main answer is not null
    tb = solve_nulls_values_in_schwartz_questions(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        main_answer="like_me_agg",
        other_answers=[
            "not_like_me_agg",
            "very_much_like_me",
            "like_me",
            "somewhat_like_me",
            "a_little_like_me",
            "not_like_me",
            "not_at_all_like_me",
            "dont_know",
            "no_answer",
        ],
    )

    # Replace 0 by null for don't know columns if the rest of columns are null
    # For important in life questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=IMPORTANT_IN_LIFE_QUESTIONS,
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
        questions=INTERESTED_IN_POLITICS_QUESTIONS,
        answers=["very", "somewhat", "not_very", "not_at_all"],
    )

    # For political action questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=POLITICAL_ACTION_QUESTIONS,
        answers=["have_done", "might_do", "never"],
    )

    # For environment vs. economy question
    tb = replace_dont_know_by_null(
        tb=tb, questions=ENVIRONMENT_VS_ECONOMY_QUESTIONS, answers=["environment", "economy", "other_answer"]
    )

    # For income equality question
    tb = replace_dont_know_by_null(
        tb=tb, questions=INCOME_EQUALITY_QUESTIONS, answers=["equality", "neutral", "inequality"]
    )

    # For "Schwartz" questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        answers=["very_much_like_me", "like_me", "somewhat_like_me", "a_little_like_me", "not_like_me"],
    )

    # For "Work vs. leisure" question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORK_VS_LEISURE_QUESTIONS,
        answers=["work", "leisure", "neutral"],
    )

    # For work questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORK_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree"],
    )

    # For most serious problem of the world question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=MOST_SERIOUS_PROBLEM_QUESTIONS,
        answers=["poverty", "women_discr", "sanitation", "education", "pollution"],
    )

    # For justifiable questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=JUSTIFIABLE_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral"],
    )

    # For worries questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORRIES_QUESTIONS,
        answers=["very_much", "a_great_deal", "not_much", "not_at_all"],
    )

    # For happiness questions
    tb = replace_dont_know_by_null(
        tb=tb, questions=HAPPINESS_QUESTIONS, answers=["very", "quite", "not_very", "not_at_all"]
    )

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

        # if null_check is False and f"dont_know_{q}" is 0, set f"dont_know_{q}" as null
        tb.loc[(~tb["null_check"]) & (tb[f"dont_know_{q}"] == 0), f"dont_know_{q}"] = float("nan")

    # Remove null_check
    tb = tb.drop(columns=["null_check"])

    return tb


def solve_nulls_values_in_schwartz_questions(
    tb: Table,
    questions: list,
    main_answer: str,
    other_answers: List[str],
) -> Table:
    """
    Replace null values in Schwartz questions by 0 when the main answer is not null
    """

    for q in questions:
        # Add q to each member of answers
        main_answer_by_question = f"{main_answer}_{q}"
        other_answers_by_question = [f"{a}_{q}" for a in other_answers]

        # Assign 0 to each other_answers_by_question when it's null and when main_answer_by_question is not null
        for a in other_answers_by_question:
            tb.loc[(tb[main_answer_by_question].notnull()) & (tb[a].isnull()), a] = 0

    return tb


def sanity_checks(tb: Table) -> Table:
    """
    Perform sanity checks on the data
    """
    # Check if the sum of the answers is 100
    # For important in life questions
    tb = check_sum_100(
        tb=tb,
        questions=IMPORTANT_IN_LIFE_QUESTIONS,
        answers=["very", "rather", "not_very", "notatall", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For interested in politics question
    tb = check_sum_100(
        tb=tb,
        questions=INTERESTED_IN_POLITICS_QUESTIONS,
        answers=["very", "somewhat", "not_very", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For political action questions
    tb = check_sum_100(
        tb=tb,
        questions=POLITICAL_ACTION_QUESTIONS,
        answers=["have_done", "might_do", "never", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For environment vs. economy question
    tb = check_sum_100(
        tb=tb,
        questions=ENVIRONMENT_VS_ECONOMY_QUESTIONS,
        answers=["environment", "economy", "other_answer", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For income equality question
    tb = check_sum_100(
        tb=tb,
        questions=INCOME_EQUALITY_QUESTIONS,
        answers=["equality", "neutral", "inequality", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For "Schwartz" questions
    tb = check_sum_100(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        answers=[
            "very_much_like_me",
            "like_me",
            "somewhat_like_me",
            "a_little_like_me",
            "not_like_me",
            "not_at_all_like_me",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For "Work vs. leisure" question
    tb = check_sum_100(
        tb=tb,
        questions=WORK_VS_LEISURE_QUESTIONS,
        answers=["work", "leisure", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For work questions
    tb = check_sum_100(
        tb=tb,
        questions=WORK_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For most serious problem of the world question
    tb = check_sum_100(
        tb=tb,
        questions=MOST_SERIOUS_PROBLEM_QUESTIONS,
        answers=["poverty", "women_discr", "sanitation", "education", "pollution", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For justifiable questions
    tb = check_sum_100(
        tb=tb,
        questions=JUSTIFIABLE_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For worries questions
    tb = check_sum_100(
        tb=tb,
        questions=WORRIES_QUESTIONS,
        answers=["very_much", "a_great_deal", "not_much", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For happiness questions
    tb = check_sum_100(
        tb=tb,
        questions=HAPPINESS_QUESTIONS,
        answers=["very", "quite", "not_very", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    return tb


def check_sum_100(tb: Table, questions: list, answers: list, margin: float) -> Table:
    """
    Check if the sum of the answers is 100
    """
    for q in questions:
        # Add q to each member of answers
        answers_by_question = [f"{a}_{q}" for a in answers]

        # Check if all columns in answers_by_question are null
        tb["sum_check"] = tb[answers_by_question].sum(axis=1)

        # Create mask to check if sum is 100
        mask = ((tb["sum_check"] <= 100 - margin) | (tb["sum_check"] >= 100 + margin)) & tb[
            answers_by_question
        ].notnull().all(axis=1)
        tb_error = tb[mask].reset_index(drop=True).copy()

        if not tb_error.empty:
            log.fatal(
                f"""{len(tb_error)} answers for {q} are not adding up to 100%:
                {tabulate(tb_error[['country', 'year'] + answers_by_question + ['sum_check']], headers = 'keys', tablefmt = TABLEFMT, floatfmt=".1f")}"""
            )

    # Remove sum_check
    tb = tb.drop(columns=["sum_check"])

    return tb
