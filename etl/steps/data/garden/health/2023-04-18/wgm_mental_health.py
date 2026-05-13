"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from shared import (
    MAPPING_AGE_VALUES,
    MAPPING_COLUMN_NAMES,
    MAPPING_GENDER_VALUES,
    MAPPING_QUESTION_VALUES,
)
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)
# Minimum number of participants answering a question. Questions for demographic groups with fewer answers are filtered out.
# This mostly affects very granular breakdowns (e.g. by age and gender)
THRESHOLD_ANSWERS = 100


def run() -> None:
    ds_meadow = paths.load_dataset("wgm_mental_health")
    tb = ds_meadow.read("wgm_mental_health")

    # Keep relevant columns, unpivot, harmonize country names.
    tb = clean_table(tb)
    # Remove empty answers.
    tb = tb[tb["answer"] != " "]
    # Build table with shares of answers to each question across geographies and demographics.
    tb = make_share_answers(tb)
    # Map IDs to labels (question, age and gender groups, answers).
    tb = map_ids_to_labels(tb)
    # Filter out questions with low participation.
    tb = filter_rows_with_low_participation(tb)
    # Set the final dimension index.
    tb = final_formatting(tb)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def clean_table(tb: Table) -> Table:
    """Keep relevant columns, unpivot, harmonize country names."""
    columns_rel = list(MAPPING_COLUMN_NAMES.keys()) + list(MAPPING_QUESTION_VALUES.keys())
    tb = tb[columns_rel]
    tb = tb.rename(columns=MAPPING_COLUMN_NAMES)

    tb = tb.melt(
        id_vars=list(MAPPING_COLUMN_NAMES.values()),
        value_vars=list(MAPPING_QUESTION_VALUES.keys()),
        var_name="question",
        value_name="answer",
    )
    tb = tb.astype({"question": str, "answer": str})
    tb = paths.regions.harmonize_names(tb=tb, country_col="country", countries_file=paths.country_mapping_path)
    return tb


def make_share_answers(tb: Table) -> Table:
    """Build a table with shares of answers, computed for countries, World, continents and income groups."""
    tb_countries = _share_answers_for_geographies(tb)

    tb_world = tb.assign(country="World")
    tb_world = _share_answers_for_geographies(tb_world, "weight_inter_country")

    tb_continents = _reassign_to_continents(tb)
    tb_continents = _share_answers_for_geographies(tb_continents, "weight_inter_country")

    tb_incomes = _reassign_to_incomes(tb)
    tb_incomes = _share_answers_for_geographies(tb_incomes, "weight_inter_country")

    return pr.concat([tb_countries, tb_world, tb_continents, tb_incomes], ignore_index=True)


def _reassign_to_continents(tb: Table) -> Table:
    tb = tb.copy()
    continents = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]
    for continent in continents:
        countries = geo.list_countries_in_region(continent)
        mask = tb["country"].isin(countries)
        tb.loc[mask, "country"] = continent

    unc_countries = set(tb["country"]).difference(continents)
    assert not unc_countries, f"Some countries do not belong to any continent: {unc_countries}!"
    return tb


def _reassign_to_incomes(tb: Table) -> Table:
    tb = tb.copy()
    incomes = [
        "High-income countries",
        "Upper-middle-income countries",
        "Lower-middle-income countries",
        "Low-income countries",
    ]
    for income in incomes:
        countries = geo.list_countries_in_region(income)
        mask = tb["country"].isin(countries)
        tb.loc[mask, "country"] = income

    # WB left Venezuela unclassified in 2021 due to lacking data.
    unc_countries_expected = {"Venezuela"}
    unc_countries = set(tb["country"]).difference(incomes)
    assert unc_countries == unc_countries_expected, (
        f"Only Venezuela is expected to be unclassified, but found {unc_countries} to be unclassified!"
    )
    tb = tb[~tb["country"].isin(unc_countries_expected)]
    return tb


def _share_answers_for_geographies(tb: Table, weight_column: str = "weight_intra_country") -> Table:
    tb_gender_age = _share_answers(tb, dimensions=["gender", "age_group"], weight_column=weight_column)
    tb_gender = _share_answers(tb, dimensions=["gender"], weight_column=weight_column)
    tb_age = _share_answers(tb, dimensions=["age_group"], weight_column=weight_column)
    tb_nb = _share_answers(tb, weight_column=weight_column)

    tb_combined = pr.concat([tb_nb, tb_gender, tb_age, tb_gender_age], ignore_index=True)

    sums = tb_combined.groupby(["country", "year", "question", "gender", "age_group"], observed=True)[["share"]].sum()
    assert sums[abs(sums.share - 100) > 0.1].empty, (
        "The share was not correctly estimated! Sum of shares does not sum up to 100% (we allow for 0.1% error)"
    )
    return tb_combined


def _share_answers(tb: Table, weight_column: str, dimensions: list[str] = []) -> Table:
    """Return per-question/answer share + count for the given demographic dimensions."""
    columns_index_base = ["country", "year", "question", "answer"]
    columns_index = columns_index_base + dimensions

    # Named aggregation keeps columns flat and preserves origins from `weight_column`.
    tb_ = (
        tb.groupby(columns_index, observed=True)
        .agg(
            sum=(weight_column, "sum"),
            count=(weight_column, "count"),
        )
        .reset_index()
    )

    columns_normalise = [col for col in columns_index if col != "answer"]
    tb_["sum_denominator"] = tb_.groupby(columns_normalise, observed=True)["sum"].transform("sum")
    tb_["share"] = 100 * tb_["sum"] / tb_["sum_denominator"]

    dimensions_all = ["gender", "age_group"]
    for dim in dimensions_all:
        if dim not in dimensions:
            tb_[dim] = "all"

    return tb_[columns_index_base + dimensions_all + ["share", "count"]]


def map_ids_to_labels(tb: Table) -> Table:
    """Replace numeric IDs in question/answer/gender/age_group with human-readable labels."""
    # The same answer ID means different things depending on the question, so build a `question__answer -> label` map.
    question_answer_id_to_label = {}
    for q_id, q_props in MAPPING_QUESTION_VALUES.items():
        for a_id, a_label in q_props["answers"].items():
            question_answer_id_to_label[f"{q_id}__{a_id}"] = a_label
    tb["question__answer"] = tb["question"] + "__" + tb["answer"]

    _sanity_check_question(tb)
    # Drop question mh7b — too granular for the dataset.
    tb = tb[tb["question"] != "mh7b"]
    _sanity_check_answer(tb)
    _sanity_check_gender(tb)
    _sanity_check_age_ids(tb)

    question_id_to_label = {k: f"{k} - {v['title']}" for k, v in MAPPING_QUESTION_VALUES.items()}
    tb["question"] = tb["question"].replace(question_id_to_label)
    tb["answer"] = tb["question__answer"].map(question_answer_id_to_label).fillna("Unknown")
    tb["gender"] = tb["gender"].replace(MAPPING_GENDER_VALUES)
    tb["age_group"] = tb["age_group"].replace(MAPPING_AGE_VALUES)
    return tb


def _sanity_check_question(tb: Table) -> None:
    questions = set(tb["question"])
    questions_unexpected = questions.difference(set(MAPPING_QUESTION_VALUES))
    assert not questions_unexpected, f"Unexpected question ID {questions_unexpected}"


def _sanity_check_answer(tb: Table) -> None:
    q_a_map = {k: set(v["answers"]) for k, v in MAPPING_QUESTION_VALUES.items()}
    tb_q_to_a = tb.groupby("question")["answer"].apply(set).to_dict()
    for k, v in tb_q_to_a.items():
        answers_unexpected = v.difference(q_a_map[k])
        answers_missing = q_a_map[k].difference(v)
        if answers_unexpected:
            raise ValueError(f"{k}: unexpected answers {answers_unexpected}")
        if answers_missing:
            raise ValueError(f"{k}: missing answers {answers_missing}")


def _sanity_check_gender(tb: Table) -> None:
    expected = set(MAPPING_GENDER_VALUES) | {"all"}
    gender_unexpected = set(tb["gender"]).difference(expected)
    gender_missing = expected.difference(tb["gender"])
    if gender_unexpected:
        raise ValueError(f"Unexpected gender ID {gender_unexpected}")
    if gender_missing:
        raise ValueError(f"Missing gender ID {gender_missing}")


def _sanity_check_age_ids(tb: Table) -> None:
    expected = set(MAPPING_AGE_VALUES) | {"all"}
    age_unexpected = set(tb["age_group"]).difference(expected)
    age_missing = expected.difference(tb["age_group"])
    if age_unexpected:
        raise ValueError(f"Unexpected age group ID {age_unexpected}")
    if age_missing:
        raise ValueError(f"Missing age group ID {age_missing}")


def filter_rows_with_low_participation(tb: Table) -> Table:
    """Drop rows where the demographic group answered too few times to be representative."""
    num_samples_initially = len(tb)
    col_idx = ["country", "year", "question", "gender", "age_group"]
    tb["count_total"] = tb.groupby(col_idx, observed=True)["count"].transform("sum")
    tb = tb[tb["count_total"] > THRESHOLD_ANSWERS]
    percentage_kept = round(100 * len(tb) / num_samples_initially, 2)
    log.info(f"wgm_mental_health: keeping {percentage_kept}% of rows after low-participation filter")
    return tb


def final_formatting(tb: Table) -> Table:
    """Keep relevant columns and set the dimension index."""
    return tb[["country", "year", "question", "answer", "gender", "age_group", "share", "count"]].set_index(
        ["country", "year", "question", "answer", "gender", "age_group"], verify_integrity=True
    )
