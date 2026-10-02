"""Load a meadow dataset and create a garden dataset."""

from datetime import date

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from sklearn.linear_model import LinearRegression

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Constants for defining the time periods.
REFERENCE_DATE = pd.Timestamp("1949-01-01")
START_DATE = pd.Timestamp("1950-01-01")
DL_ERA_START_DATE = pd.Timestamp("2010-01-01")
END_DATE = pd.Timestamp(date.today())


def run() -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"].reset_index()

    # Run regression analysis and concatenate results
    tb_trend = run_regression(tb)
    tb = tb.drop("frac_year", axis=1)
    tb = pr.concat([tb_trend, tb])

    # Format the table
    tb = tb.format(["days_since_1949", "model"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")


def fit_exponential(models, metric):
    """Fit an exponential model to the given metric data. Code provided by Epoch AI team."""
    x = models["frac_year"].values.reshape(-1, 1)
    y = pr.to_numeric(models[metric], errors="coerce").to_numpy(dtype=float, na_value=np.nan)

    # Filter out non-positive values
    positive_mask = y > 0
    x = x[positive_mask]
    y = y[positive_mask]

    # Apply log10 transformation
    y = np.log10(y)

    # Filter out infinite and extremely large values
    finite_mask = np.isfinite(y) & (y < np.finfo(np.float32).max)
    x = x[finite_mask]
    y = y[finite_mask]

    assert len(y) >= 2, f"At least two valid observations are required to fit {metric}."
    assert len(np.unique(x)) >= 2, f"At least two distinct dates are required to fit {metric}."

    # Fit linear regression model
    reg = LinearRegression().fit(x, y)
    assert np.isfinite(reg.intercept_) and np.isfinite(reg.coef_[0]), f"Non-finite regression fit for {metric}."
    return reg.intercept_, reg.coef_[0]


def run_regression(tb):
    """Run regression analysis on the given table and return the updated table."""
    # Add fractional year for sorting and processing
    publication_dates = tb["publication_date"]
    tb.loc[:, "frac_year"] = to_fractional_year(publication_dates)
    tb = tb.sort_values(by="frac_year")

    # Define the two eras using exact dates. The same dates are used both to
    # calculate and to plot each fitted line, so its displayed slope matches its label.
    periods = [
        (START_DATE, DL_ERA_START_DATE, False),
        (DL_ERA_START_DATE, END_DATE, True),
    ]

    metrics = ["training_computation_petaflop", "parameters", "training_dataset_size__total"]
    trend_rows = {}

    for metric in metrics:
        for period_start, period_end, include_end in periods:
            period_dates = pd.Series([period_start, period_end])
            period_years = to_fractional_year(period_dates).to_numpy()
            period_name = f"{period_start.year}–{period_end.year}"
            condition = tb["frac_year"].between(
                period_years[0], period_years[1], inclusive="both" if include_end else "left"
            )

            # Subset data for the current period
            period_data = tb[condition]

            # Fit exponential model
            fit = fit_exponential(period_data, metric)
            oom_per_year = fit[1]
            info = f"{10**oom_per_year:.1f}x/year"

            # Log the results
            paths.log.info(f"{period_name} ({metric}): {info}")

            # Calculate and store the fitted values at those same boundary dates.
            line = 10 ** (fit[0] + period_years * fit[1])
            assert np.isfinite(line).all() and (line > 0).all(), f"Invalid regression predictions for {metric}."

            days_since_1949 = (period_dates - REFERENCE_DATE).dt.days
            model = f"{info} between {period_name}"
            for day, value in zip(days_since_1949, line):
                key = (int(day), model)
                trend_rows.setdefault(key, {"days_since_1949": int(day), "model": model})[metric] = value

    # Metrics can occasionally have the same rounded trend label. Coalesce their
    # values onto the same two rows so the output index remains unique.
    tb_new = pd.DataFrame(trend_rows.values())
    assert not tb_new.duplicated(subset=["days_since_1949", "model"]).any(), "Duplicate regression points found."
    assert (tb_new.groupby("model")["days_since_1949"].nunique() == 2).all(), (
        "Each regression trend must have two endpoints."
    )

    # Convert to OWID Table and add metadata
    tb_new = Table(tb_new, short_name=paths.short_name)
    for column in tb_new.columns:
        tb_new[column].metadata.origins = tb["publication_date"].metadata.origins

    return tb_new


def to_fractional_year(dates):
    """Convert dates to the decimal-year convention used by the Epoch regressions."""
    return dates.dt.year + (dates.dt.month - 1) / 12 + (dates.dt.day - 1) / 365
