"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from shared import (
    MAPPING_AGE_VALUES,
    MAPPING_COLUMN_NAMES,
    MAPPING_GENDER_VALUES,
    MAPPING_QUESTION_VALUES,
)
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Minimum number of participants answering a question. Questions for demographic groups with fewer answers are filtered out.
# This mostly affects very granular breakdowns (e.g. by age and gender)
THRESHOLD_ANSWERS = 100


def run(dest_dir: str) -> None:
    log.info("wgm_mental_health: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("wgm_mental_health"))

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wgm_mental_health"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Keep relevant columns, unpivot dataframe, harmonize country names.
    df = clean_df(df)
    # Remove empty answers
    df = df[df["answer"] != " "]
    # Build dataframe with shares of answers to each questions.
    df = make_df_with_share_answers(df)
    # Map IDs to labels (question, age and gender groups, answers)
    df = map_ids_to_labels(df)
    # Filter out questions with low participation
    df = filter_rows_with_low_participation(df)
    # Format dataframe with appropriate columns, indexes
    df = final_formatting(df)
    # Create a new table based on the dataframe `df`
    tb_garden = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Add explanation to dataset description
    ds_garden.metadata.sources[0].description += (
        "\n\nNote 1: Data for answers where the demographic group had less than 100 participants are filtered out."
        "\n\nNote 2: Empty answers have been filtered out. Empty answers may appear because the question was not applicable to the respondent or the respondent did not answer the question."
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wgm_mental_health: end")


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Keep relevant columns, unpivot dataframe, harmonise country names."""
    log.info("wgm_mental_health: cleaning dataframe")
    # Relevant columns
    columns_rel = list(MAPPING_COLUMN_NAMES.keys()) + list(MAPPING_QUESTION_VALUES.keys())
    df = df[columns_rel]
    # Rename columns
    df = df.rename(columns=MAPPING_COLUMN_NAMES)
    # Unpivot
    log.info("wgm_mental_health: unpivotting dataframe")
    df = df.melt(
        id_vars=list(MAPPING_COLUMN_NAMES.values()),
        value_vars=list(MAPPING_QUESTION_VALUES.keys()),
        var_name="question",
        value_name="answer",
    )
    # Set dtypes of all question columns to strings
    df = df.astype({"question": str, "answer": str})
    # Harmonise country names
    log.info("wgm_mental_health: harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    return df


def make_df_with_share_answers(df: pd.DataFrame) -> pd.DataFrame:
    """Build dataframe with shares of answers to each questions.

    Obtains values for all countries, continents and income groups. Also for various demographic groups (age, gender)
    """
    # Obtain dataframe for countries
    log.info("wgm_mental_health: building dataframe for countries")
    df_countries = _make_df_with_share_answers(df)
    # Obtain dataframe for World
    log.info("wgm_mental_health: building dataframe for World")
    df_world = df.assign(country="World")
    df_world = _make_df_with_share_answers(df_world, "weight_inter_country")
    # Obtain dataframe for continents
    log.info("wgm_mental_health: building dataframe for continents")
    df_continents = _build_df_with_continents(df)
    df_continents = _make_df_with_share_answers(df_continents, "weight_inter_country")
    # Obtain dataframe for income groups
    log.info("wgm_mental_health: building dataframe for income groups")
    df_incomes = _build_df_with_incomes(df)
    df_incomes = _make_df_with_share_answers(df_incomes, "weight_inter_country")
    # Merge
    df = pd.concat([df_countries, df_world, df_continents, df_incomes], ignore_index=True)
    return df


def _build_df_with_continents(df: pd.DataFrame) -> pd.DataFrame:
    # Obtain dataframe for continents
    df_continents = df.copy()
    continents = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
    df_continents["country"] = df_continents["country"].cat.add_categories(continents)
    for continent in continents:
        countries = geo.list_countries_in_region(continent)
        msk = df_continents["country"].isin(countries)
        df_continents.loc[msk, "country"] = continent

    # Sanity check
    unc_countries = set(df_continents.country).difference(continents)
    assert not unc_countries, f"Some countries do not belong to any continent: {unc_countries}!"
    return df_continents


def _build_df_with_incomes(df: pd.DataFrame) -> pd.DataFrame:
    # Obtain dataframe for income groups
    df_income = df.copy()
    incomes = [
        "High-income countries",
        "Upper-middle-income countries",
        "Lower-middle-income countries",
        "Low-income countries",
    ]
    df_income["country"] = df_income["country"].cat.add_categories(incomes)
    for income in incomes:
        countries = geo.list_countries_in_region(income)
        msk = df_income["country"].isin(countries)
        df_income.loc[msk, "country"] = income

    # Sanity check
    unc_countries_expected = {"Venezuela"}  # WB unclassified Venezuela in 2021, bc of lacking data
    unc_countries = set(df_income.country).difference(incomes)
    assert (
        unc_countries == unc_countries_expected
    ), f"Only Venezuela is expected to be unclassified, but found {unc_countries} to be unclassified!"
    # Remove Venezuela
    df_income = df_income[~df_income["country"].isin(unc_countries_expected)]
    return df_income


def _make_df_with_share_answers(df: pd.DataFrame, weight_column: str = "weight_intra_country") -> pd.DataFrame:
    # 1. broken down by gender and age group
    df_gender_age = _make_individual_df_with_share_answers(
        df, dimensions=["gender", "age_group"], weight_column=weight_column
    )
    # 2. broken down by gender
    df_gender = _make_individual_df_with_share_answers(df, dimensions=["gender"], weight_column=weight_column)
    # 3. broken down by age_group
    df_age = _make_individual_df_with_share_answers(df, dimensions=["age_group"], weight_column=weight_column)
    # 4. no breakdown
    df_nb = _make_individual_df_with_share_answers(df, weight_column=weight_column)
    # Combine dataframes
    log.info("wgm_mental_health: combining dataframes into combined one")
    df_combined = pd.concat([df_nb, df_gender, df_age, df_gender_age], ignore_index=True)
    # Sanity check
    x = df_combined.groupby(["country", "year", "question", "gender", "age_group"], observed=True)[["share"]].sum()
    assert x[
        abs(x.share - 100) > 0.1
    ].empty, "The share was not correctly estimated! Sum of shares does not sum up to 100% (we allow for 0.1% error)"
    return df_combined


def _make_individual_df_with_share_answers(
    df: pd.DataFrame, weight_column: str, dimensions: List[str] = []
) -> pd.DataFrame:
    """Obtain table with answer percentages and counts to each question for all demographic groups and countries.

    For each question, obtain the number of each registered answer. Also, obtain the "weighted number", which is given
    by weighting each answer according to the `weight_column` column. This is later used to obtain the share.

    The `weight_column` has either the value of "weight_intra_country" (used to standardise different demographics within a country) or
    "weight_inter_country" (to standardise a variable across different countries)
    """

    log.info(f"wgm_mental_health: building dataframe with share of answers for dimensions {dimensions}")
    operations = ["sum", "count"]
    columns_index_base = ["country", "year", "question", "answer"]
    columns_index = columns_index_base + dimensions
    df_ = df.groupby(columns_index, observed=True).agg({weight_column: operations})
    df_.columns = operations
    df_ = df_.reset_index()
    # For each question, now obtain the percentage of each of its answers (weighted).
    columns_normalise = [col for col in columns_index if col not in ["answer"]]
    df_["sum_denominator"] = df_.groupby(columns_normalise, observed=True)["sum"].transform("sum")
    df_["share"] = 100 * df_["sum"] / df_["sum_denominator"]
    # Add missing dimensions
    dimensions_all = ["gender", "age_group"]
    for dim in dimensions_all:
        if dim not in dimensions:
            df_[dim] = "all"
    # Format df
    df_ = df_[
        columns_index_base + dimensions_all + ["share", "count"]
    ]  # DEBUG: return columns "sum", "sum_denominator", too
    return df_


def map_ids_to_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Instead of having IDs we use labels.

    The source uses IDs for gender and age groups, answers and questions.
    """
    # Answer ID to Answer label mapping
    # Create unique identifier for answer id. Note that answer ids mean different things depending on the question!
    # Therefore, we build a mapping `questionId__answerId -> answerLabel`
    question_answer_id_to_label = {}
    for q_id, q_props in MAPPING_QUESTION_VALUES.items():
        for a_id, a_label in q_props["answers"].items():
            question_answer_id_to_label[f"{q_id}__{a_id}"] = a_label
    df["question__answer"] = df["question"] + "__" + df["answer"]

    log.info("wgm_mental_health: sanity checking IDs (question, answer, gender, age_group)}")
    # Sanity checks (Question)
    _sanity_check_question(df)
    # Drop question mh7b (it is too granular)
    df = df[df["question"] != "mh7b"]

    # Sanity checks for (question, answer) pairs
    _sanity_check_answer(df)
    # Sanity check gender IDs
    _sanity_check_gender(df)
    # Sanity checkk age_group IDs
    _sanity_check_age_ids(df)

    # Question ID to Question label mapping
    question_id_to_label = {k: f"{k} - {v['title']}" for k, v in MAPPING_QUESTION_VALUES.items()}

    # Map IDs to Labels (Question, Answer, Gender, Age group)
    log.info("wgm_mental_health: mapping ids to labels}")
    df["question"] = df["question"].replace(question_id_to_label)
    df["answer"] = df["question__answer"].map(question_answer_id_to_label).fillna("Unknown")
    df["gender"] = df["gender"].replace(MAPPING_GENDER_VALUES)
    df["age_group"] = df["age_group"].replace(MAPPING_AGE_VALUES)

    return df


def _sanity_check_question(df: pd.DataFrame):
    q_map = {k: v["title"] for k, v in MAPPING_QUESTION_VALUES.items()}
    questions = set(df["question"])
    questions_unexpected = questions.difference(set(MAPPING_QUESTION_VALUES))
    assert not questions_unexpected, f"Unexpected question ID {questions_unexpected}"
    questions_missing = set(q_map).difference(questions)
    assert not questions_unexpected, f"Missing question ID {questions_missing}"


def _sanity_check_answer(df: pd.DataFrame):
    q_a_map = {k: set(v["answers"]) for k, v in MAPPING_QUESTION_VALUES.items()}
    dfg = df.groupby("question")["answer"].apply(set).to_dict()
    for k, v in dfg.items():
        answers_unexpected = v.difference(q_a_map[k])
        answers_missing = q_a_map[k].difference(v)
        if answers_unexpected:
            print(k, "unexpected", answers_unexpected)
            # if answers_unexpected != {" "}:
            raise ValueError("Would expect nothing or whitespace, this is new!")
        if answers_missing:
            print(k, "missing", answers_missing)
            # if answers_missing != {" "}:
            raise ValueError("Would expect nothing or whitespace, this is new!")


def _sanity_check_gender(df: pd.DataFrame):
    gender_unexpected = set(df["gender"]).difference(set(MAPPING_GENDER_VALUES) | {"all"})
    gender_missing = set(set(MAPPING_GENDER_VALUES) | {"all"}).difference(df["gender"])
    if gender_unexpected:
        raise ValueError(f"Unexpected gender ID {gender_unexpected}")
    if gender_missing:
        raise ValueError(f"Missing gender ID {gender_missing}")


def _sanity_check_age_ids(df: pd.DataFrame):
    age_unexpected = set(df["age_group"]).difference(set(MAPPING_AGE_VALUES) | {"all"})
    age_missing = set(set(MAPPING_AGE_VALUES) | {"all"}).difference(df["age_group"])
    if age_unexpected:
        raise ValueError(f"Unexpected age group ID {age_unexpected}")
    if age_missing:
        raise ValueError(f"Missing age group ID {age_missing}")


def filter_rows_with_low_participation(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows where the number of answers for a question from a demographic group was very low"""
    log.info("wgm_mental_health: Filtering entries with few participants.")
    num_samples_initially = len(df)
    col_idx = ["country", "year", "question", "gender", "age_group"]
    df["count_total"] = df.groupby(col_idx, observed=True)["count"].transform("sum")
    df = df[df["count_total"] > THRESHOLD_ANSWERS]
    # Alternative would be to just remove share values and keep absolute counts
    # df.loc[df["count_total"] > THRESHOLD_ANSWERS, "Share"] = None
    # Log
    percentage_kept = round(100 * len(df) / num_samples_initially, 2)
    log.info(f"wgm_mental_health: Keeping {percentage_kept}% of all the rows.")
    return df


def final_formatting(df: pd.DataFrame) -> pd.DataFrame:
    """Keep relevant rows and set index."""
    log.info("wgm_mental_health: final formatting}")
    df = df[["country", "year", "question", "answer", "gender", "age_group", "share", "count"]].set_index(
        ["country", "year", "question", "answer", "gender", "age_group"], verify_integrity=True
    )
    return df
