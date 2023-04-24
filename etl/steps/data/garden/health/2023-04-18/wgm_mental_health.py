"""Load a meadow dataset and create a garden dataset."""

from typing import List

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

from .shared import age_group_mapping, column_rename, gender_mapping, question_mapping

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference: Dataset = paths.load_dependency("reference")
    tb_countries_regions = ds_reference["countries_regions"]

    return tb_countries_regions


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators: Dataset = paths.load_dependency(channel="garden", namespace="demography", short_name="population")
    tb_population = ds_indicators["population"]

    return tb_population


def run(dest_dir: str) -> None:
    log.info("wgm_mental_health: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wgm_mental_health")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wgm_mental_health"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Create a new table with the processed data.
    tb_garden = make_table(df)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wgm_mental_health: end")


def make_table(df: pd.DataFrame) -> Table:
    """Create a new table from a dataframe."""
    df = clean_df(df)
    df = make_df_with_share_answers(df)
    df = map_ids_to_labels(df)
    df = final_formatting(df)
    tb = Table(df, short_name=paths.short_name, underscore=True)
    return tb


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    log.info("wgm_mental_health: cleaning dataframe")
    # Relevant columns
    columns_rel = list(column_rename.keys()) + list(question_mapping.keys())
    df = df[columns_rel]
    # Rename columns
    df = df.rename(columns=column_rename)
    # Unpivot
    log.info("wgm_mental_health: unpivotting dataframe")
    df = df.melt(
        id_vars=list(column_rename.values()),
        value_vars=list(question_mapping.keys()),
        var_name="question",
        value_name="answer",
    )
    # Set dtypes of all question columns to strings
    df = df.astype({"question": str, "answer": str})
    # Harmonise country names
    log.info("wgm_mental_health: harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    return df


def make_df_with_share_answers(df: pd.DataFrame) -> pd.DataFrame:
    # 1. broken down by gender and age group
    df_gender_age = make_individual_df_with_share_answers(df, ["gender", "age_group"])
    # 2. broken down by gender
    df_gender = make_individual_df_with_share_answers(df, ["gender"])
    # 3. broken down by age_group
    df_age = make_individual_df_with_share_answers(df, ["age_group"])
    # 4. no breakdown
    df_nb = make_individual_df_with_share_answers(df)
    # Combine dataframes
    log.info("wgm_mental_health: combining dataframes into combined one")
    df_combined = pd.concat([df_nb, df_gender, df_age, df_gender_age], ignore_index=True)
    # Sanity check
    x = df_combined.groupby(["country", "year", "question", "gender", "age_group"], observed=True)[["share"]].sum()
    assert x[
        x.share - 100 > 0.1
    ].empty, "The share was not correctly estimated! Sum of shares does not sum up to 100% (we allow for 0.1% error)"
    return df_combined


def make_individual_df_with_share_answers(df: pd.DataFrame, dimensions: List[str] = []) -> pd.DataFrame:
    log.info(f"wgm_mental_health: building dataframe with share of answers for dimensions {dimensions}")
    # For each question, obtain the number of each registered answer. Also, obtain the "weighted number", which is given
    # by weighting each answer according to the `weight_inter_country` variable (used to standardise different demographics)
    operations = ["sum", "count"]
    columns_index_base = ["country", "year", "question", "answer"]
    columns_index = columns_index_base + dimensions
    df_ = df.groupby(columns_index, observed=True).agg({"weight_intra_country": operations})
    df_.columns = operations
    df_ = df_.reset_index()
    # For each question, now obtain the percentage of each of its answers (weighted).
    columns_normalise = [col for col in columns_index if col not in ["answer"]]
    df_weights_sum = df_.groupby(columns_normalise, as_index=False)[["sum"]].sum()
    df_ = df_.merge(df_weights_sum, on=columns_normalise, suffixes=("", "_denominator"))
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
    # Answer ID to Answer label mapping
    # Create unique identifier for answer id. Note that answer ids mean different things depending on the question!
    # Therefore, we build a mapping `questionId__answerId -> answerLabel`
    question_answer_id_to_label = {}
    for q_id, q_props in question_mapping.items():
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
    question_id_to_label = {k: v["title"] for k, v in question_mapping.items()}

    # Map IDs to Labels (Question, Answer, Gender, Age group)
    log.info("wgm_mental_health: mapping ids to labels}")
    df["question"] = df["question"].replace(question_id_to_label)
    df["answer"] = df["question__answer"].map(question_answer_id_to_label).fillna("Unknown")
    df["gender"] = df["gender"].replace(gender_mapping)
    df["age_group"] = df["age_group"].replace(age_group_mapping)

    return df


def _sanity_check_question(df: pd.DataFrame):
    q_map = {k: v["title"] for k, v in question_mapping.items()}
    questions = set(df["question"])
    questions_unexpected = questions.difference(set(question_mapping))
    assert not questions_unexpected, f"Unexpected question ID {questions_unexpected}"
    questions_missing = set(q_map).difference(questions)
    assert not questions_unexpected, f"Missing question ID {questions_missing}"


def _sanity_check_answer(df: pd.DataFrame):
    q_a_map = {k: set(v["answers"]) for k, v in question_mapping.items()}
    dfg = df.groupby("question")["answer"].apply(set).to_dict()
    for k, v in dfg.items():
        answers_unexpected = v.difference(q_a_map[k])
        answers_missing = q_a_map[k].difference(v)
        if answers_unexpected:
            print(k, "unexpected", answers_unexpected)
            if answers_unexpected != {" "}:
                raise ValueError("ERROR. Would expect nothing or whitespace, this is new!")
        if answers_missing:
            print(k, "missing", answers_missing)
            if answers_missing != {" "}:
                raise ValueError("ERROR. Would expect nothing or whitespace, this is new!")


def _sanity_check_gender(df: pd.DataFrame):
    gender_unexpected = set(df["gender"]).difference(set(gender_mapping) | {"all"})
    gender_missing = set(set(gender_mapping) | {"all"}).difference(df["gender"])
    if gender_unexpected:
        raise ValueError(f"Unexpected gender ID {gender_unexpected}")
    if gender_missing:
        raise ValueError(f"Missing gender ID {gender_missing}")


def _sanity_check_age_ids(df: pd.DataFrame):
    age_unexpected = set(df["age_group"]).difference(set(age_group_mapping) | {"all"})
    age_missing = set(set(age_group_mapping) | {"all"}).difference(df["age_group"])
    if age_unexpected:
        raise ValueError(f"Unexpected age group ID {age_unexpected}")
    if age_missing:
        raise ValueError(f"Missing age group ID {age_missing}")


def final_formatting(df: pd.DataFrame) -> pd.DataFrame:
    # Format
    log.info("wgm_mental_health: final formatting}")
    df = df[["country", "year", "question", "answer", "gender", "age_group", "share", "count"]].set_index(
        ["country", "year", "question", "answer", "gender", "age_group"], verify_integrity=True
    )
    return df