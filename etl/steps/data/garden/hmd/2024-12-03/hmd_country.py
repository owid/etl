"""Load a meadow dataset and create a garden dataset."""

import calendar

import numpy as np
import pandas as pd

from etl.data_helpers import geo
from etl.data_helpers.misc import interpolate_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hmd_country")
    ds_hmd = paths.load_dataset("hmd")

    # Read table from meadow dataset.
    tb = ds_meadow.read("monthly")
    tb_pop = ds_hmd.read("population")

    #
    # Process data.
    #
    tb = make_main_table(tb, tb_pop)
    tb_month_long, tb_month_dimensions, tb_month_max = make_tables(tb)

    tables = [
        tb_month_long.format(["country", "date"], short_name="birth_rate"),
        tb_month_dimensions.format(["country", "year", "month"], short_name="birth_rate_month"),
        tb_month_max.format(["country", "year"], short_name="birth_rate_month_max"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_main_table(tb, tb_pop):
    ## Discard unknown/total values
    tb = clean_table(tb)

    # Add population to monthly birth data table
    tb = add_population_column(tb, tb_pop)

    # Estimate metrics
    tb = estimate_metrics(tb)

    # Fix date (use last day instead of first)
    tb["date"] = tb["date"] + pd.to_timedelta(tb["days_in_month"] - 1, unit="D")

    # Sort rows
    tb = tb.sort_values(["country", "date", "date"])

    return tb


def make_tables(tb):
    # Classic time-series, with date-values
    tb_long = tb[["country", "date", "birth_rate", "birth_rate_per_day"]]
    ## Add 9-month lead
    tb_long["date_lead"] = (tb_long[["date"]] - pd.DateOffset(months=9) + pd.offsets.MonthEnd(0)).squeeze()
    cols = [col for col in tb_long.columns if col != "date_lead"]
    cols_lead = [col for col in tb_long.columns if col != "date"]
    tb_long = tb_long[cols].merge(
        tb_long[cols_lead],
        left_on=["country", "date"],
        right_on=["country", "date_lead"],
        suffixes=("", "_lead_9months"),
        how="outer",
    )
    tb_long["date"] = tb_long["date"].fillna(tb_long["date_lead"])
    tb_long = tb_long.drop(columns="date_lead")

    # Month as a dimension
    tb_dimensions = tb[["country", "year", "month", "birth_rate", "birth_rate_per_day"]]
    tb_dimensions["month"] = tb_dimensions["month"].apply(lambda x: calendar.month_name[x])

    # For each year, ID of the month with highest birth rate per day
    def find_peak_month(tb):
        tb = tb.loc[
            tb.groupby(["country", "year"])["birth_rate_per_day"].idxmax(),
            ["country", "year", "month", "birth_rate_per_day"],
        ].rename(columns={"month": "month_max", "birth_rate_per_day": "birth_rate_per_day_max"})
        tb["month_max_name"] = tb["month_max"].apply(lambda x: calendar.month_name[x])

        return tb

    tb_month_max = find_peak_month(tb)
    # Get: "for a given year, what was the month that lead to highest month-birth rate in +9months"
    tb_pre9m = tb.copy()
    tb_pre9m["date"] = (tb_pre9m[["date"]] - pd.DateOffset(months=9) + pd.offsets.MonthEnd(0)).squeeze()
    tb_pre9m["year"] = tb_pre9m["date"].dt.year
    tb_pre9m["year"] = tb_pre9m["year"].copy_metadata(tb["year"])
    tb_pre9m["month"] = tb_pre9m["date"].dt.month
    tb_pre9m["month"] = tb_pre9m["month"].copy_metadata(tb["month"])

    tb_pre9m = find_peak_month(tb_pre9m)
    # Merge
    tb_month_max = tb_month_max.merge(tb_pre9m, on=["country", "year"], how="outer", suffixes=("", "_lead_9months"))
    return tb_long, tb_dimensions, tb_month_max


def clean_table(tb):
    """Filter rows, harmonize country names, add date column."""
    # Filter unwanted month categories, set dtype
    tb = tb.loc[~tb["month"].isin(["TOT", "UNK"])]
    tb["month"] = tb["month"].astype("Int64")
    ## Create date column. TODO: check what day of the month to assign
    tb["date"] = pd.to_datetime(tb[["year", "month"]].assign(day=1))
    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unknown_excluded_countries=False,
    )

    return tb


def add_population_column(tb, tb_pop):
    """Add population column to main table for each date."""
    # Prepare population table
    tb_pop = _prepare_population_table(tb_pop)
    # Merge population table with main table
    tb = tb.merge(tb_pop, on=["country", "date"], how="outer")
    tb = tb.sort_values(["country", "date"])
    # Interpolate to get monthly population estimates
    tb_ = interpolate_table(
        tb[["country", "date", "population"]],
        entity_col="country",
        time_col="date",
        time_mode="none",
    )
    tb = tb.drop(columns="population").merge(tb_, on=["country", "date"], how="left")
    # Drop unused rows
    tb = tb.dropna(subset=["year"])
    return tb


def _prepare_population_table(tb):
    """Prepare population table to merge with main table.

    Original table is given in years, but we need it in days! We use linear interpolation for that.
    """
    tb_aux = tb.loc[(tb["age"] == "total") & (tb["sex"] == "total"), ["country", "year", "population"]]
    ## Assign a day to population. TODO: Check if this is true
    tb_aux["date"] = pd.to_datetime(tb_aux["year"].astype(str) + "-01-01")
    tb_aux = tb_aux.drop(columns="year")

    return tb_aux


def estimate_metrics(tb):
    """Estimate metrics: birth rate and birth rate per day."""
    # Get days in month
    tb["days_in_month"] = tb.apply(lambda row: calendar.monthrange(row["year"], row["month"])[1], axis=1)
    # Estimate rates
    tb["birth_rate"] = tb["births"] / tb["population"] * 1_000
    tb["birth_rate_per_day"] = tb["birth_rate"] / tb["days_in_month"] * 1_000
    # Check
    assert tb[["birth_rate", "birth_rate_per_day"]].notna().all().all()
    # Replace INF values with NAs
    tb[["birth_rate", "birth_rate_per_day"]] = tb[["birth_rate", "birth_rate_per_day"]].replace(
        [np.inf, -np.inf], pd.NA
    )
    # Drop NAs
    tb = tb.dropna(subset=["birth_rate", "birth_rate_per_day"])

    return tb
