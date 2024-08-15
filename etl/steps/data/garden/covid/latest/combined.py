"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Dataset, Table
from shared import add_population_2022

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_tests = paths.load_dataset("testing")
    ds_who = paths.load_dataset("cases_deaths")

    # Read table from meadow dataset.
    tb_tests = ds_tests["testing"].reset_index()
    tb_who = ds_who["cases_deaths"].reset_index()

    # Population dataset
    ds_population = paths.load_dataset("population")

    #
    # Process data.
    #
    # Set Dtypes
    tb_tests = tb_tests.astype(
        {
            "date": "datetime64[ns]",
            "total_tests": float,
            "new_tests_7day_smoothed": float,
        }
    )
    tb_cases = tb_who.astype(
        {
            "date": "datetime64[ns]",
            "total_cases": float,
            "new_cases_7_day_avg_right": float,
        }
    )
    # Merge
    tb = tb_tests.merge(tb_cases, on=["country", "date"])

    # Estimate indicators
    tb = add_test_case_ratios(tb)

    ## Criteria: 'Has population ≥ 5M AND had ≥100 cases ≥21 days ago AND has testing data'
    tb = add_criteria(tb, ds_population)

    # Keep relevant columns
    tb = tb[
        [
            "country",
            "date",
            "cumulative_tests_per_case",
            "short_term_tests_per_case",
            "short_term_positivity_rate",
            "cumulative_positivity_rate",
            "has_population_5m_and_100_cases_and_testing_data",
        ]
    ]

    # Format
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_test_case_ratios(tb: Table) -> Table:
    """Estimate positive rate and tests-per-case.

    tests per case: num_tests / num_cases
    positive rate (case per test): num_cases / num_tests

    NOTE: all is smoothed by a 7-day rolling window average.
    """
    tb["cumulative_tests_per_case"] = tb["total_tests"] / tb["total_cases"]
    tb["short_term_tests_per_case"] = tb["new_tests_7day_smoothed"] / tb["new_cases_7_day_avg_right"]
    ## Cases per tests
    tb["short_term_positivity_rate"] = 100 * tb["new_cases_7_day_avg_right"] / tb["new_tests_7day_smoothed"]
    tb["cumulative_positivity_rate"] = 100 * tb["total_cases"] / tb["total_tests"]

    # Replace infinite values
    cols = [
        "cumulative_tests_per_case",
        "short_term_tests_per_case",
        "short_term_positivity_rate",
        "cumulative_positivity_rate",
    ]
    for col in cols:
        tb[col] = tb[col].replace([np.inf, -np.inf], np.nan)

    tb = tb.sort_values(["date"])
    for col in cols:
        tb[col] = (
            tb.groupby("country", observed=True)[col]
            .rolling(
                window=7,
                min_periods=1,
                center=False,
            )
            .mean()
            .reset_index(level=0, drop=True)
        )

    return tb


def add_criteria(tb: Table, ds_population: Dataset) -> Table:
    tb = add_population_2022(tb, ds_population)
    mask = (
        (tb["days_since_100_total_cases"].notnull())
        & (tb["days_since_100_total_cases"] >= 21)
        & (tb["population"] >= 5_000_000)
    )
    tb["has_population_5m_and_100_cases_and_testing_data"] = 0
    tb.loc[mask, "has_population_5m_and_100_cases_and_testing_data"] = 1
    tb["has_population_5m_and_100_cases_and_testing_data"] = tb[
        "has_population_5m_and_100_cases_and_testing_data"
    ].copy_metadata(tb["days_since_100_total_cases"])
    tb = tb.drop(columns="population")

    return tb
