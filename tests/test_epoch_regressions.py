import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
from owid.catalog import Table

STEP_PATH = Path(__file__).parents[1] / "etl/steps/data/garden/artificial_intelligence/2025-03-12/epoch_regressions.py"


def load_step_module():
    spec = importlib.util.spec_from_file_location("epoch_regressions", STEP_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_regression_values_and_dates_use_the_same_boundaries():
    step = load_step_module()
    dates = pd.to_datetime(
        [
            "1950-07-02",
            "1970-01-01",
            "2009-11-22",
            "2010-05-13",
            "2018-01-01",
            step.END_DATE,
        ]
    )
    fractional_years = step.to_fractional_year(pd.Series(dates)).to_numpy()

    rates = {
        "training_computation_petaflop": (1.5, 4.3),
        "parameters": (1.2, 2.2),
        "training_dataset_size__total": (1.3, 2.9),
    }
    data = {
        "publication_date": dates,
        "days_since_1949": (dates - step.REFERENCE_DATE).days,
    }
    for metric, (early_rate, recent_rate) in rates.items():
        data[metric] = np.where(
            dates < step.DL_ERA_START_DATE,
            early_rate ** (fractional_years - step.START_DATE.year),
            recent_rate ** (fractional_years - step.DL_ERA_START_DATE.year),
        )

    trends = step.run_regression(Table(pd.DataFrame(data)))

    for metric, (early_rate, recent_rate) in rates.items():
        for start, end, rate in [
            (step.START_DATE, step.DL_ERA_START_DATE, early_rate),
            (step.DL_ERA_START_DATE, step.END_DATE, recent_rate),
        ]:
            model = f"{rate:.1f}x/year between {start.year}–{end.year}"
            actual = trends[trends["model"] == model].sort_values("days_since_1949")
            expected_days = (pd.Series([start, end]) - step.REFERENCE_DATE).dt.days.tolist()

            assert actual["days_since_1949"].tolist() == expected_days
            assert len(actual) == 2
            assert actual[metric].notna().all()


def test_fit_exponential_rejects_insufficient_observations():
    step = load_step_module()
    models = Table(pd.DataFrame({"frac_year": [2020.0], "metric": [1.0]}))

    with np.testing.assert_raises_regex(AssertionError, "At least two valid observations"):
        step.fit_exponential(models, "metric")


def test_fit_exponential_handles_nullable_values():
    step = load_step_module()
    models = Table(
        pd.DataFrame(
            {
                "frac_year": [2020.0, 2021.0, 2022.0],
                "metric": pd.Series([1.0, pd.NA, 4.0], dtype="Float64"),
            }
        )
    )

    _, slope = step.fit_exponential(models, "metric")

    assert np.isclose(10**slope, 2.0)
