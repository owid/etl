"""Load a meadow dataset and create a garden dataset."""

from datetime import UTC, datetime, timedelta

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Mapping values
MAPPED_VALUES = {
    "binary": {"No": 0, "Yes": 100},
    "quasi_binary": {"No, neither": 0, "Yes, one dose": 100, "Yes, two doses": 100},
    "frequency": {"Not at all": 0, "Rarely": 0, "Sometimes": 0, "Frequently": 100, "Always": 100},
    "easiness": {
        "Very easy": 0,
        "Somewhat easy": 0,
        "Neither easy nor difficult": 0,
        "Somewhat difficult": 100,
        "Very difficult": 100,
    },
    "willingness": {
        "Very unwilling": 0,
        "Somewhat unwilling": 0,
        "Neither willing nor unwilling": 0,
        "Somewhat willing": 100,
        "Very willing": 100,
    },
    "scariness": {
        "I am not at all scared that I will contract the Coronavirus (COVID-19)": 0,
        "I am not very scared that I will contract the Coronavirus (COVID-19)": 0,
        "I am fairly scared that I will contract the Coronavirus (COVID-19)": 100,
        "I am very scared that I will contract the Coronavirus (COVID-19)": 100,
    },
    "happiness": {
        "Much less happy now": 0,
        "Somewhat less happy now": 0,
        "About the same": 0,
        "Somewhat more happy now": 100,
        "Much more happy now": 100,
    },
    "handling": {"Very badly": 0, "Somewhat badly": 0, "Somewhat well": 100, "Very well": 100},
    "agreement": {"1 – Disagree": 0, "2": 0, "3": 0, "4": 0, "5": 100, "6": 100, "7 - Agree": 100},
    "agreement5_reverse": {"1 - Strongly agree": 100, "2": 100, "3": 0, "4": 0, "5 – Strongly disagree": 0},
    "disagreement5_reverse": {"1 - Strongly agree": 0, "2": 0, "3": 0, "4": 100, "5 – Strongly disagree": 100},
    "neutral5_reverse": {"1 - Strongly agree": 0, "2": 0, "3": 100, "4": 0, "5 – Strongly disagree": 0},
    "trustworthiness": {"1 - Not at all trustworthy": 0, "2": 0, "3": 0, "4": 100, "5 - Completely trustworthy": 100},
    "efficiency": {"1 - Not efficient at all": 0, "2": 0, "3": 0, "4": 100, "5 - Extremely efficient": 100},
    "unity": {"More divided": 0, "No change": 0, "More united": 100},
    "strength": {"Very weak": 0, "Somewhat weak": 0, "Somewhat strong": 100, "Very strong": 100},
    "increase": {"Decreased": 0, "No change": 0, "Increased": 100},
    "importance": {
        "Not at all important": 0,
        "A little important": 0,
        "Moderately important": 100,
        "Very important": 100,
    },
    "quantity": {"Not at all": 0, "A little": 0, "Moderately": 100, "Very much": 100},
}
# FREQ: temporal level at which to aggregate the individual survey
# responses, passed as the `freq` argument to
# pandas.Series.dt.to_period. Must conform to a valid Pandas offset
# string (e.g. 'M' = "month", "W" = "week").
FREQ = "M"
# MIN_RESPONSES: country-date-question observations with less than this
# many valid responses will be dropped. If "None", no observations will
# be dropped.
MIN_RESPONSES = 100


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("yougov")

    # Read table from meadow dataset.
    tb = ds_meadow["yougov"].reset_index()
    tb_mapping = ds_meadow["yougov_extra_mapping"].reset_index()
    tb_composite = ds_meadow["yougov_composite"].reset_index()

    #
    # Process data.
    #
    tb = process_columns(tb, tb_mapping)
    tb = derive_cols(tb, tb_mapping)
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = aggregate_table(tb, tb_mapping)

    # Format
    tb = tb.format(["country", "date", "question"], short_name="yougov")
    tb_composite = tb_composite.format(["country", "date"], short_name="yougov_composite")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_composite,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_columns(tb: Table, tb_mapping: Table) -> Table:
    """Preprocess columns."""
    tb_mapping = tb_mapping.loc[tb_mapping["preprocess"].notnull()]
    mapping = tb_mapping.set_index("code_name")["preprocess"].to_dict()
    for code_name, preprocess in mapping.items():
        if code_name in tb.columns:
            tb.loc[:, code_name] = tb[code_name].replace(MAPPED_VALUES[preprocess])
            uniq_values = set(MAPPED_VALUES[preprocess].values())
            assert (
                tb.loc[:, code_name].drop_duplicates().dropna().isin(uniq_values).all()
            ), f"One or more non-NaN values in {code_name} are not in {uniq_values}"
    return tb


def derive_cols(tb: Table, tb_mapping: Table) -> Table:
    """Derive columns."""
    derived_variables_to_keep = tb_mapping[tb_mapping["derived"] & tb_mapping["keep"]].code_name.unique().tolist()
    if "covid_vaccinated_or_willing" in derived_variables_to_keep:
        # constructs the covid_vaccinated_or_willing variable
        # pd.crosstab(df['vac'].fillna(-1), df['vac_1'].fillna(-1))
        vac_min_val = min(
            MAPPED_VALUES[
                tb_mapping.loc[
                    tb_mapping["code_name"] == "covid_vaccine_received_one_or_two_doses",
                    "preprocess",
                ].squeeze()
            ].values()
        )
        vac_max_val = max(
            MAPPED_VALUES[
                tb_mapping.loc[
                    tb_mapping["code_name"] == "covid_vaccine_received_one_or_two_doses",
                    "preprocess",
                ].squeeze()
            ].values()
        )
        vac_1_max_val = max(
            MAPPED_VALUES[
                tb_mapping.loc[
                    tb_mapping["code_name"] == "willingness_covid_vaccinate_this_week",
                    "preprocess",
                ].squeeze()
            ].values()
        )

        assert not (
            (tb["covid_vaccine_received_one_or_two_doses"] == vac_max_val)
            & tb["willingness_covid_vaccinate_this_week"].notnull()
        ).any(), (
            "Expected all vaccinated respondents to NOT be asked whether they would "
            "get vaccinated, but found at least one vaccinated respondent who was "
            "asked the latter question."
        )
        assert not (
            (tb["covid_vaccine_received_one_or_two_doses"] == vac_min_val)
            & tb["willingness_covid_vaccinate_this_week"].isnull()
        ).any(), (
            "Expected all unvaccinated respondents to be asked whether they would "
            "get vaccinated, but found at least one unvaccinated respondent who was "
            "not asked the latter question."
        )

        tb.loc[:, "covid_vaccinated_or_willing"] = (
            (tb["covid_vaccine_received_one_or_two_doses"] == vac_max_val)
            | (tb["willingness_covid_vaccinate_this_week"] == vac_1_max_val)
        ).astype(int) * vac_max_val
        tb.loc[
            tb["covid_vaccine_received_one_or_two_doses"].isnull()
            & tb["willingness_covid_vaccinate_this_week"].isnull(),
            "covid_vaccinated_or_willing",
        ] = np.nan

    return tb


def aggregate_table(tb: Table, tb_mapping: Table) -> Table:
    """Estimate aggregate values.

    Given is data at user-level. This function aggregates all responses per day.

    Provides day-average and day-counts for each question.
    """
    s_period = tb["date"].dt.to_period(FREQ)
    if FREQ == "M":
        tb.loc[:, "date_mid"] = s_period.dt.start_time.dt.date + timedelta(days=14)
    else:
        tb.loc[:, "date_mid"] = (s_period.dt.start_time + (s_period.dt.end_time - s_period.dt.start_time) / 2).dt.date
    today = datetime.now(UTC).date()
    if tb["date_mid"].max() > today:
        tb.loc[:, "date_mid"] = tb["date_mid"].replace({tb["date_mid"].max(): today})

    questions = [q for q in tb_mapping["code_name"].tolist() if q in tb.columns]

    # Set dtypes
    tb[questions] = tb[questions].astype("Int64")

    # computes the mean for each country-date-question observation
    # (returned in long format)
    tb_avg = (
        tb.groupby(["country", "date_mid"], as_index=False, observed=True)[questions]
        .mean()
        .melt(id_vars=["country", "date_mid"], var_name="question", value_name="average")
        .rename(columns={"date_mid": "date"})
    )

    # counts the number of non-NaN responses for each country-date-question
    # observation (returned in long format)
    tb_ = tb[["country", "date_mid"] + questions].copy()
    tb_[questions] = tb_[questions].notnull()
    tb_counts = (
        tb_.groupby(["country", "date_mid"], as_index=False, observed=True)[questions]
        .sum()
        .melt(id_vars=["country", "date_mid"], var_name="question", value_name="num_responses")
        .rename(columns={"date_mid": "date"})
    )

    tb = tb_avg.merge(tb_counts, on=["country", "date", "question"], how="outer")
    return tb
